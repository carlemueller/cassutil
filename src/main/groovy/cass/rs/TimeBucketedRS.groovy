package cass.rs

import cass.drv.Drv
import cass.drv.Rs
import cass.drv.St
import cass.util.RowU
import com.datastax.driver.core.ColumnDefinitions
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import jsonutil.JSONUtil

/**
 * This result set is intended to provide query services for a group of sharded tables with an implicit order to the
 * data shards. For example, ticker's scheduled table could be split across time-shards in different tables for easy
 * truncation rather than wrestling with compaction, but we'd still want to be able to query across all the tables
 * as if they were one table.
 *
 * This assumes two tiers of time buckets: a series of short-term small buckets (14 x 12 hr buckets) that "churn" rapidly
 * and a set of longer-term (24 x 30 day) long term buckets. Those assumptions are specific to the ticker app.
 *
 * On a daily basis there should be a process that transfers data from the current long term bucket into its forthcoming
 * 2x 12hr short term buckets. There are four inactive buckets: two for staging, one that is purged, and one "grace period"
 *
 * On a "monthly"/30dy basis, the purgable long term bucket should be truncated. Every 12 hrs the purgable short term
 * bucket should be truncated.... It may be technically possible that relying on TTLs we wouldn't need to do any active
 * truncation.
 *
 * With buffer buckets, there should be about 43 tables total.
 *
 */
@Slf4j
@CompileStatic
class TimeBucketedRS implements Rs<Row> {

    Drv drv

    String group
    long startSecond
    long currentMillis = System.currentTimeMillis()

    String keyspace = 'ticker'
    String shortTablePrefix = "scheduled_st_"
    String longTablePrefix = "scheduled_lt_"

    long millisPerShort = 1000L * 60L * 60L * 12L  //12hr
    long millisPerLong = 1000L * 60L * 60L * 24L * 30L //30dy
    int activeShortTermBuckets = 14
    // have five inactive, and only do rolling truncate of the middle inactive to avoid boundaries. Need five if
    // daily job to populate two forthcoming buckets. Might be able to get away with four (two for prep, one for
    // purge, one for grace, but five buffer buckets would give two grace, but we only have code to check one grace)
    int shortTermBuckets = activeShortTermBuckets + 5
    int activeLongTermBuckets = 24
    // have three inactive, and only do rolling truncate of the middle inactive to avoid boundaries
    int longTermBuckets = activeLongTermBuckets + 3

    String consistency
    Integer threshold
    Integer fetchSize

    // required init call

    // call post-instantiation once everything is set up in the properties
    TimeBucketedRS init() {
        assert drv: "Cass driver not set"
        assert startSecond: "second not specified to start queries from"
        bucketQueries = bucketQueries(startSecond, currentMillis)
        assert bucketQueries?.size() > 0 : "starting second $startSecond has no active bucket for current time $currentMillis"
        curBucketRS = drv.execSync(bucketQueries[0])
        bucketQueryIndex = 0
        initialized = true

        return this
    }

    String buildCQL(String tableName) {
        "SELECT * from ${keyspace ? keyspace + '.' : ""}${tableName} WHERE group = ? AND second >= ?"
    }

    // --- resultSet implementation methods

    // primary-only: lets pretend we don't have long --> short delay
    // --> double-write only the furthest active short term bucket with the most recent long-term bucket
    private boolean initialized = false
    private ResultSet curBucketRS
    private ResultSet nextBucketRS
    private List<St> bucketQueries
    private int bucketQueryIndex = 0

    boolean ready() { initialized }
    ColumnDefinitions getColumnDefinitions() {curBucketRS.columnDefinitions}

    Row one() {
        log.trace "TBRS.one :: top"
        if (!initialized) throw new IllegalStateException('TimeBucketedRS not yet initialized')
        fetchMore()
        Row row = curBucketRS.one()
        log.trace "TBRS.one :: "+ (row == null ? null : JSONUtil.toJSON(RowU.toList(row)))
        return row
    }

    /**
     * handle all cases:
     * - no threshold specified (disables nextBucket prefetching, next bucket is only )
     * - fetching next set of data in current bucket
     * - current bucket is done pulling rows, so start pulling the next bucket if possible
     *
     * @return
     */
    boolean fetchMore() {
        log.trace "TBRS.fetchThreshold :: top, check exhausted ${bucketQueryIndex} ${bucketQueries.get(bucketQueryIndex)?.cql}"
        // What could possibly go wrong ?? :-)

        // check if it's time for the next bucket to become the current bucket
        if (curBucketRS.exhausted) {
            log.trace "TBRS.fetchThreshold :: cur is exhausted"
            // are there more queries in the query list to perform?
            if (bucketQueryIndex < bucketQueries.size() - 1) {
                log.trace "TBRS.fetchThreshold ::   more bucketqueries"
                // is the next query started?
                if (nextBucketRS) {
                    log.trace "TBRS.fetchThreshold ::     nextRs ready"
                    // make the next query the current one
                    bucketQueryIndex++
                    curBucketRS = nextBucketRS
                    nextBucketRS = null
                    // ... what if the next bucket is empty? recurse to force another pull
                    if (curBucketRS.availableWithoutFetching == 0) {
                        log.trace "TBRS.fetchThreshold ::       RECURSING since just-pulled query has no rows"
                        // recurse. we shouldn't have too many tables because of cassandra limits, so stack depth
                        // shouldn't be a danger
                        return fetchMore()
                    }
                    log.trace "TBRS.fetchThreshold :: return true 1, nextRS has become curRS and should have processable rows"
                    return true
                } else {
                    log.trace "TBRS.fetchThreshold ::     nextRs NOT ready"
                    bucketQueryIndex++
                    curBucketRS = drv.execSync(bucketQueries[bucketQueryIndex])
                    log.trace "TBRS.fetchThreshold ::     curRS has been queried for $bucketQueryIndex ${bucketQueries.get(bucketQueryIndex)?.cql}"
                    if (curBucketRS.availableWithoutFetching == 0) {
                        log.trace "TBRS.fetchThreshold ::       RECURSING, just-pulled query has no rows"
                        // recurse. we shouldn't have too many tables because of cassandra limits, so stack depth
                        // shouldn't be a danger
                        return fetchMore()
                    }
                    log.trace "TBRS.fetchThreshold :: return true 2"
                    return true
                }
            } else {
                log.trace "TBRS.fetchThreshold :: no more buckets to retrieve and cur is exhausted"
                return false // no more buckets to retrieve
            }
        }
        // current RS is not exhausted, so has processable rows to return. check if we need to start another fetch
        // or trigger fetch of next resultSet
        log.trace "TBRS.fetchThreshold :: PREFETCH"
        if (bucketQueries[bucketQueryIndex].fetchThreshold) {
            log.trace "TBRS.fetchThreshold :: fetchThreshold configured"
            if (curBucketRS.fullyFetched) {
                log.trace "TBRS.fetchThreshold ::   fully fetched"
                if (!nextBucketRS) {
                    log.trace "TBRS.fetchThreshold ::     nextRS not ready"
                    if (bucketQueries[bucketQueryIndex].fetchThreshold >= curBucketRS.availableWithoutFetching) {
                        log.trace "TBRS.fetchThreshold ::       should get nextRS"
                        if (bucketQueryIndex < bucketQueries.size() - 1) {
                            // start pulling the next bucket
                            log.trace "TBRS.fetchThreshold ::         query still todo, start nextRS"
                            nextBucketRS = drv.execSync(bucketQueries[bucketQueryIndex + 1])
                            return true
                        } else {
                            log.trace "fetchThreshold ::         no more buckets"
                            return false // no more buckets left to prefetch
                        }
                    } else {
                        log.trace " fetchThreshold ::       "
                        return false // current result set still has a lot of rows, wait before starting next bucket
                    }
                } else {
                    return false // next bucket prefetch pulling has already been started
                }
            } else {
                // current RS NOT fully fetched
                log.trace "TBRS.fetchThreshold ::   more to fetch, see if we are under the fetch Threshold to trigger a fetch"
                if (bucketQueries[bucketQueryIndex].fetchThreshold >= curBucketRS.availableWithoutFetching) {
                    log.trace "TBRS.fetchThreshold ::     under fetch threshold, pull some more"
                    curBucketRS.fetchMoreResults()
                    return true
                } else {
                    log.trace "fetchThreshold ::     plenty remaining, no fetch triggered"
                    return false  // current set not under fetch next rows threshold
                }
            }
        } else {
            log.trace "fetchThreshold :: NO THRESHOLD"
            return false // no thresholding specified, so we don't prefetch
        }
    }

    boolean isExhausted() {
        log.trace "TBRS.exhausted :: top"
        if (!initialized) throw new IllegalStateException('TimeBucketedRS.exhausted :: not yet initialized')
        boolean exhausted = curBucketRS.exhausted && bucketQueryIndex == bucketQueries.size() - 1
        log.trace "TBRS.exhausted :: $exhausted"
        return exhausted
    }

    // ---- helper logic

    long[] bucketStartEndMillis(long millis, long millisPerBucket) {
        [((Long) millis.intdiv(millisPerBucket)) * millisPerBucket,
         ((Long) millis.intdiv(millisPerBucket) + 1) * millisPerBucket - 1L] as long[]
    }

    /**
     * calculate buckets starting from the indicated current active bucket. This is the same process currently for
     * both short and long term buckets, all that differs is the input values.
     *
     * It's probably a good idea that the total time represented by all short buckets is less than a long term bucket.
     *
     * The default is 14 days of millis of short term buckets total versus 30 days per long term bucket
     *
     * @param firstBucketMillis
     * @param startBucket
     * @param tablePrefix
     * @param activeBuckets
     * @param totalBuckets
     * @param millisPerBucket
     * @return
     */
    List<List> calculateBucketList(long firstBucketMillis, long startBucket, String tablePrefix, int activeBuckets, int totalBuckets, long millisPerBucket) {
        List<List> tables = []
        for (long i = startBucket; i < activeBuckets; i++) {
            long bucketMillis = (long) (firstBucketMillis + i * millisPerBucket)

            long[] startEnd = bucketStartEndMillis(bucketMillis, millisPerBucket)
            tables.add([
                    tablePrefix + (((Long) bucketMillis.intdiv(millisPerBucket)) % totalBuckets),
                    startEnd[0],
                    startEnd[1]])
        }
        return tables
    }

    /**
     * produce a list of queries and prepargs to step through across the rotating time buckets for both the short
     * term rotating time buckets and long-term ones. IF the requested startSecond is far enough in the future, then
     * there will be no short term buckets in the queries
     *
     * @param startSecond - start second (should we just make this Millis so both params are same unit?)
     * @param curMillis - system current time millis
     * @return List < [ cql , cqlarg ] >  - list of tuples
     */
    List<St> bucketQueries(long startSecond, long curMillis) {
        long startMillis = startSecond * 1000L
        long beginOfShortTermMillis = curMillis.intdiv(millisPerShort) * millisPerShort
        long endOfShortTermMillis = beginOfShortTermMillis + millisPerShort * ((long) activeShortTermBuckets) - 1L

        List<St> queries = []
        // allow querying of one short term post-expiration "grace bucket" before the current bucket if asking for the past buckets
        if (startMillis < beginOfShortTermMillis) {
            // default the startMillis if it is earlier than the grace bucket to the start of the grace bucket
            if (startMillis < beginOfShortTermMillis - millisPerShort) {
                startMillis = beginOfShortTermMillis - millisPerShort
            }
            long graceBucket = (long) ((beginOfShortTermMillis - millisPerShort).intdiv(millisPerShort) % shortTermBuckets)
            queries.add(new St(
                    cql: buildCQL("${shortTablePrefix}${graceBucket}"),
                    args: [group, startMillis] as Object[],
                    consistency: consistency,
                    fetchThreshold: threshold,
                    fetchSize: fetchSize))
            startMillis = beginOfShortTermMillis
        }
        // figure out if query includes short term buckets
        if (startMillis < endOfShortTermMillis) {
            long bucketsAhead = (long) ((startMillis).intdiv(millisPerShort)) - (long) curMillis.intdiv(millisPerShort)
            List<List> shortTerm = calculateBucketList(curMillis, bucketsAhead, shortTablePrefix, activeShortTermBuckets, shortTermBuckets, millisPerShort)
            List<List> longTerm = calculateBucketList(curMillis, 0, longTablePrefix, activeLongTermBuckets, longTermBuckets, millisPerLong)
            shortTerm[0][1] = startMillis
            longTerm[0][1] = ((long) shortTerm.last()[2]) + 1
            queries.addAll shortTerm.collect {
                new St(
                        cql: buildCQL(it[0].toString()),
                        args: [group, it[1]] as Object[],
                        consistency: consistency,
                        fetchThreshold: threshold,
                        fetchSize: fetchSize)
            }
            queries.addAll longTerm.collect {
                new St(
                        cql: buildCQL(it[0].toString()),
                        args: [group, it[1]] as Object[],
                        consistency: consistency,
                        fetchThreshold: threshold,
                        fetchSize: fetchSize)
            }
            return queries
        }
        // return only long-term bucket results
        long bucketsAhead = (long) ((startMillis).intdiv(millisPerLong)) - (long) curMillis.intdiv(millisPerLong)
        if (bucketsAhead > 24) {
            // too far in the future (until we implement an extreme long-term bucket
            return []
        }
        List<List> longTerm = calculateBucketList(curMillis, bucketsAhead, longTablePrefix, activeLongTermBuckets, longTermBuckets, millisPerLong)
        longTerm[0][1] = startMillis
        queries.addAll longTerm.collect {
            new St(
                    cql: buildCQL(it[0].toString()),
                    args: [group, it[1]] as Object[],
                    consistency: consistency,
                    fetchThreshold: threshold,
                    fetchSize: fetchSize)
        }
        return queries
    }

}

