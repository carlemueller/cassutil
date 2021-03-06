
package cass.drv

import cass.rs.AsyncQuerySetIterator
import cass.rs.RowArrayIterator
import com.datastax.driver.core.*
import com.datastax.driver.core.exceptions.NoHostAvailableException
import com.datastax.driver.core.policies.ConstantReconnectionPolicy
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import javax.annotation.PreDestroy
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory
import java.nio.ByteBuffer
import java.security.KeyStore
import java.security.SecureRandom
import java.time.Instant
import java.time.ZoneId
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * TODO: more complex asyncs like chained async query spray of arbitrary depth
 * TODO: update spray with failure report listing.
 * TODO: logging: restructure code into different sections with different namespaces for the logging using delegate inner classes?
 */

@CompileStatic
@Slf4j
class Drv {
    private static final String HEALTHCHECK_CQL = 'select keyspace_name from schema_keyspaces limit 1'
    private static final long DEFAULT_RECONNECTION_DELAY_MILLIS = 60000
    private static final long CLOSE_WAIT_MILLIS = 5000

    static class SSLConf {
        String contextType = "SSL"
        List<String> cipherSuites = ["TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA"]
        String truststoreType = "JKS"
        String truststorePath
        String truststorePassword
        String keystoreType = "JKS"
        String keystorePath
        String keystorePassword
    }


    List<String> nodes = ['127.0.0.1']
    Integer port = null // default is 9042
    boolean autoStart = true
    String defaultConsistency = "LOCAL_QUORUM"
    String u
    String p
    SSLConf ssl

    Cluster.Builder builder

    private boolean initialized = false
    private boolean destroyed = false
    private final Object initLock = new Object()

    private Cluster cluster = null
    private Session clusterSession = null
    protected Map<String, Session> keyspaceSessions = [:]
    protected Map<String, PreparedStatement> stmtCache = [:]


    // --- health checks

    boolean isInitialized() {
        initialized && !destroyed
    }

    long getHealthy() {
        if (!initialized || destroyed) {
            return 0
        }
        try {
            long time = System.currentTimeMillis()
            if (execSync(HEALTHCHECK_CQL, null).one() == null) {
                return 0
            }
            time = System.currentTimeMillis() - time
            return time == 0 ? 1 : time
        } catch (Exception e) {
            log.warn("healthcheck $defaultConsistency fail: ${e.message} ", e)
            return 0
        }
    }

    long getAllHealthy() {
        if (!initialized || destroyed) {
            return 0
        }
        try {
            long time = System.currentTimeMillis()
            if (execSync(new St(cql: HEALTHCHECK_CQL, consistency: 'ALL')).one() == null){
                return 0
            }
            time = System.currentTimeMillis() - time
            return time == 0 ? 1 : time
        } catch (Exception e) {
            log.warn("healthcheck-ALL fail: ${e.message} ", e)
            return 0
        }
    }

    List<Object[]> clusterStatus() {
        if (!initialized || destroyed) {
            return null
        }
        List<Object[]> status = []
        for (Host host: cluster.metadata.allHosts) {
            status.add([host.address,host.up] as Object[])
        }
        return status
    }

    Set<String> getKeyspaces() {
        if (!initialized || destroyed) {
            return null
        }
        Set<String> keyspaceNames = [] as Set
        cluster.metadata.keyspaces.each { keyspaceNames.add(it.name) }
        return keyspaceNames
    }

    // --- query/update execution

    // async: can use Futures.addCallback to the returned ResultSetFuture
    // https://www.datastax.com/dev/blog/java-driver-async-queries
    // ... groovy?
    ResultSetFuture execAsync(String cql, Object[] args) {
        St st = new St(cql: cql, args: args)
        execAsync(st)
    }

    ResultSetFuture execAsync(St st) {
        execAsync(getSession(st.keyspace),st)
    }

    ResultSetFuture execAsync(Session session, St st) {
        prepSt(st)
        long start = System.currentTimeMillis()
        ResultSetFuture rs = session.executeAsync(st.stmt)
        long stop = System.currentTimeMillis()
        log.debug("execAsync response: ${stop-start}ms")
        log.debug("  cql: ${st.cql ?: cqlFromStmt(st.stmt)}")
        log.debug("  args: ${st.args}")
        return rs
    }

    List<ResultSetFuture> asyncSpray(List<St> stList, String consistency, Long timestamp) {
        List<ResultSetFuture> results = []
        stList.each {
            if (consistency) { it.consistency = consistency }
            if (timestamp) { it.timestamp = timestamp }
            results.add(execAsync(it))
        }
        return results
    }

    ResultSet execSync(String cql, Object[] args) {
        St st = new St(cql: cql, args: args)
        execSync(st)
    }

    ResultSet execSync(St st) {
        execSync(getSession(st.keyspace),st)
    }

    ResultSet execSync(Session session, St st) {
        prepSt(st)
        long start = System.currentTimeMillis()
        ResultSet rs = session.execute(st.stmt)
        long stop = System.currentTimeMillis()
        log.debug("execSync response: ${stop-start}ms")
        log.debug("  cql: ${st.cql ?: cqlFromStmt(st.stmt)}")
        log.debug("  args: ${st.args}")
        return rs
    }

    List<Object[]> syncAll(String cql, Object[] args) {
        List<Object[]> results = []
        Iterator<Object[]> iter = syncIterator(cql, args)
        while (iter.hasNext()) {
            results.add(iter.next())
        }
        return results
    }

    List<Object[]> syncAll(St st) {
        List<Object[]> results = []
        Iterator<Object[]> iter = syncIterator(st)
        while (iter.hasNext()) {
            results.add(iter.next())
        }
        return results
    }

    Iterator<Object[]> syncIterator(String cql, Object[] args) {
        syncIterator(new St(cql:cql, args:args))
    }

    Iterator<Object[]> syncIterator(St st) {
        new RowArrayIterator(resultSet: execSync(st), fetchThreshold: st.fetchThreshold)
    }

    /**
     * By taking a cql statement and an interator of prep args for that statement, async
     * single-row queries (the cql must return only a single row, other rows will be ignored)
     * are "sprayed" using the driver's token awareness.
     *
     * The queries are executed in "batches" or "query sets". This defaults to 40 per set.
     *
     * TODO: efficiency testing.
     * TODO: v2: group tokens together
     *
     * @param cql           preppable statement
     * @param argsIterator  series of arguments to invoke the prepped cql statement with
     * @return              an iterator that returns rows
     */
    Iterator<Object[]> asyncSprayQuery(String cql, Iterator<Object[]> argsIterator) {
        asyncSprayQuery(new St(cql:cql,fetchSize: 40, fetchThreshold: 20), argsIterator)
    }

    Iterator<Object[]> asyncSprayQuery(St st, Iterator<Object[]> argsIterator) {
        return new AsyncQuerySetIterator(this, st, argsIterator)
    }

    // --- preparation and conversion

    PreparedStatement prepare(String cql) {
        PreparedStatement stmt = stmtCache[cql]
        if (!stmt) {
            stmt = session.prepare(cql)
            stmtCache[cql] = stmt
        }
        return stmt
    }

    void prepSt(St st) {
        if (!st.stmt) {
            st.stmt = makeStmt(st.cql, st.args, st.consistency, st.timestamp)
        } else {
            if (st.timestamp) {
                st.stmt.defaultTimestamp = st.timestamp
            }
            if (st.consistency) {
                st.stmt.consistencyLevel = ConsistencyLevel.valueOf(st.consistency)
            }
        }
        if (st.fetchSize) {
            st.stmt.fetchSize = st.fetchSize
        }

    }

    Statement makeStmt(String cql, Object[] prepArgs, String consistency, Long usingTimestamp) {
        try {
            // TODO: nullcheck option to avoid tombstone production with null values
            Statement stmt = prepArgs ? prepare(cql).bind(typeConvertPrepArgs(prepArgs)) : new SimpleStatement(cql)
            stmt.consistencyLevel = ConsistencyLevel.valueOf(consistency ?: defaultConsistency)
            if (usingTimestamp) {
                stmt.defaultTimestamp = usingTimestamp
            }
            return stmt
        } catch (Exception e) {
            log.error("stmt prep: $cql $prepArgs", e)
            // TODO: log and throw with cql and prepArgs bound
            throw e
        }
    }

    static Object[] typeConvertPrepArgs(Object[] args) {
        for (int i = 0; i < args.length; i++) {
            Object obj = args[i]
            if (obj instanceof byte[] || obj instanceof Byte[]) {
                args[i] = ByteBuffer.wrap((byte[]) obj)
            } else if (obj instanceof java.util.Date) {
                args[i] = Instant.ofEpochMilli(((java.util.Date) obj).getTime()).atZone(ZoneId.systemDefault()).toLocalDate()
            }
        }
        return args
    }

    static String cqlFromStmt(Statement stmt) {
        if (stmt instanceof SimpleStatement) {
            return ((SimpleStatement)stmt).queryString
        }
        if (stmt instanceof BoundStatement) {
            return ((BoundStatement)stmt).preparedStatement().queryString
        }
        if (stmt instanceof BatchStatement) {
            StringBuilder sb = new StringBuilder()
            ((BatchStatement)stmt).statements.each {sb.append(cqlFromStmt(it))}
            return sb.toString()
        }
        return "Unknown statement type: ${stmt.class}"
    }

    // --- startup

    Drv initWrapExistingCluster(Cluster cluster) {
        synchronized (initLock) {
            if (!initialized) {
                assert cluster: "Drv: cannot wrap null cluster"
                checkNotShutDown()
                this.cluster = cluster
                this.clusterSession = cluster.connect()
                this.nodes = cluster.metadata.allHosts.collect { it.address.toString() }.toList()
                this.initialized = true
            }
        }
        return this
    }

    Drv initWrapExistingSession(Session session) {
        initWrapExistingCluster(session?.cluster)
    }

    Drv initDataSources() {
        if (!initialized) {
            doInitDataSources()
        }
        return this
    }

    private void checkNotShutDown() {
        if (destroyed) {
            throw new IllegalStateException("application is shutting down");
        }
    }

    Session getSession() {
        getSession(null)
    }

    Session getSession(String keyspace) {
        if (!initialized) {
            if (autoStart) {
                initDataSources()
            } else {
                throw new IllegalStateException("driver not initialized")
            }
        }
        checkNotShutDown()
        if (keyspace == null) {
            return clusterSession
        }
        if (!keyspaceSessions.containsKey(keyspace)) {
            Session sess = cluster.connect(keyspace)
            keyspaceSessions[keyspace] = sess
            return sess
        } else {
            return keyspaceSessions[keyspace]
        }
    }

    private Cluster.Builder defaultPolicyBuilder() {
        Cluster.Builder builder = Cluster.builder()
        // default load balance is token-aware(dc-round-robin)
        log.info("DRV INIT: defaulting to LZ4 compression")
        builder = builder.withCompression(ProtocolOptions.Compression.LZ4)
        log.info("DRV INIT: defaulting to constant time reconnection policy")
        builder = builder.withReconnectionPolicy(new ConstantReconnectionPolicy(DEFAULT_RECONNECTION_DELAY_MILLIS))
        log.info("DRV INIT: defaulting to downgrading retry policy")
        builder.withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
    }

    private void doInitDataSources() {
        synchronized (initLock) {
            checkNotShutDown();
            if (!initialized) {
                try {
                    Cluster.Builder builder = this.builder ?: defaultPolicyBuilder()
                    if (u || p) {
                        builder = builder.withCredentials(u,p)
                    }
                    log.info("DRV INIT: contact nodes: $nodes")
                    builder = builder.addContactPoints(nodes as String[])
                    if (port != null) {
                        log.info("DRV INIT: port: $port")
                        builder = builder.withPort(port)
                    }
                    if (ssl) {
                        configureSSL(builder,ssl)
                    }
                    // build cluster
                    cluster = builder.build()
                    clusterSession = cluster.connect()
                    Metadata metadata = cluster.getMetadata();
                    log.info("DRV: Connected to cluster: ${metadata.clusterName}")
                    for (Host host : metadata.getAllHosts()) {
                        log.info("DRV: Datacenter: ${host.datacenter}; Host: ${host.address}; Rack: ${host.rack}; Status: ${host.up ? 'up' : 'down'}")
                    }
                    initialized = true;
                } catch (NoHostAvailableException e) {
                    log.error("initDataSourceFailed: ${e.getMessage()}; not stopping context initialization", e)
                }
            }
        }
    }

    private static void configureSSL(Cluster.Builder builder, SSLConf sslConf) {
        if (sslConf) {
            builder.withSSL(JdkSSLOptions.builder().withSSLContext(makeSSLContext(sslConf)).withCipherSuites(sslConf.cipherSuites as String[]).build())
        } else {
            builder.withSSL()
        }
    }

    private static SSLContext makeSSLContext(SSLConf sslConf) {
        File checkKeystore = new File(sslConf.truststorePath)
        assert new File(sslConf.keystorePath)?.exists() : "key store ${sslConf.keystorePath} aka ${checkKeystore.canonicalPath} not found"
        File checkTruststore = new File(sslConf.truststorePath)
        assert new File(sslConf.truststorePath)?.exists() : "trust store ${sslConf.truststorePath} aka ${checkTruststore.canonicalPath} not found"

        SSLContext sslctx = SSLContext.getInstance(sslConf.contextType)
        FileInputStream tsf = new FileInputStream(sslConf.truststorePath)
        KeyStore ts = KeyStore.getInstance(sslConf.truststoreType)
        ts.load(tsf, sslConf.truststorePassword?.toCharArray())
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
        tmf.init(ts)

        FileInputStream ksf = new FileInputStream(sslConf.keystorePath)
        KeyStore ks = KeyStore.getInstance(sslConf.keystoreType)
        ks.load(ksf, sslConf.keystorePassword?.toCharArray())
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

        kmf.init(ks, sslConf.keystorePassword?.toCharArray())

        sslctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom())
        return sslctx
    }

    // --- shutdown
    @PreDestroy
    protected void destroy() {
        destroyed = true

        if (session != null) {
            handleClose(session.closeAsync())
        }
        if (cluster != null) {
            handleClose(cluster.closeAsync());
        }
    }

    private static void handleClose(CloseFuture closeFuture) {
        try {
            closeFuture.get(CLOSE_WAIT_MILLIS, TimeUnit.MILLISECONDS)
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            log.warn(e.getMessage(), e)
        }

        if (!closeFuture.isDone()) {
            closeFuture.force()
        }
    }

    // --- accessors
    Cluster getCluster() {
        if (!initialized && autoStart) {
            initDataSources()
        }
        return cluster
    }

    // --- builder methods and legacy setters

    Drv nodes(String nodesCSV) {
        nodes = nodesCSV?.split(',').toList()
        return this
    }

    Drv nodes(List<String> nodeList) {
        nodes = nodeList
        return this
    }

    Drv port(Integer port) {
        this.port = port
        return this
    }

    void setClusterContactNodes(String nodesCSV) {
        nodes = nodesCSV?.split(',').toList()
    }

    void setClusterContactPort(int port) {
        this.port = port
    }

    void setClusterPort(String port) {
        if (port?.equalsIgnoreCase("DEFAULT")) {
            this.port = 9042
        } else {
            this.port = Integer.parseInt(port)
        }
    }

}

// runs two query sets (in theory): one is being processed while the other is being retrieved
// ASSUMES SINGLE-ROW-RESULT QUERIES BEING TOKEN-AWARE SPRAY-CALLED


