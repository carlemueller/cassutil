package cass.drv

import com.datastax.driver.core.Statement
import groovy.transform.CompileStatic

/**
 * A convenience struct/bean to wrap cql and its various modifiers in execution, or alternatively a statement and its modifiers.
 *
 * If explicitly defined (consistency, cql, args, timestamp, etc), those will override anything set in an explicitly provided Statement, or the Statement will be reevaluated.
 *
 */

@CompileStatic
class St {
    String cql
    Object[] args
    Statement stmt // cql overrides stmt...
    String consistency
    Long timestamp
    String keyspace
    Integer fetchSize
    Integer fetchThreshold

    String getKs() { keyspace }

    void setKs(String ks) { keyspace = ks }

    St() {}

    St(String cql, Object[] args) { this.cql = cql; this.args = args }

    St(String keyspace, String cql, Object[] args) { this.keyspace = keyspace; this.cql = cql; this.args = args }

    St(String keyspace, String cql, Object[] args, String consistency, Long timestamp) {
        this.keyspace = keyspace
        this.cql = cql
        this.args = args
        this.consistency = consistency
        this.timestamp = timestamp
    }

}