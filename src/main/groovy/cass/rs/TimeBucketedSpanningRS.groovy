package cass.rs

import cass.drv.Drv
import cass.drv.Rs
import cass.drv.St
import cass.util.TimeBucketU
import com.datastax.driver.core.ColumnDefinitions
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * This RS executes a query that is supposed to span multiple time buckets of a single class of time bucket.
 *
 * It currently assumes we are doing this for the testschema scheduled table between the defaults, buildCQL(),
 * and the hardcoded assumption that the timestamp is in position 1 of the column array (indexing from 0).
 */

@Slf4j
@CompileStatic
class TimeBucketedSpanningRS implements Rs<Row> {
    Drv drv

    String group
    long startSecond
    long currentMillis

    String keyspace = 'testschema'
    String tablePrefix = "scheduled_st_"

    long millisPerBucket = 1000L * 60L * 60L * 12L  //12hr
    int activeShortTermBuckets = 14
    int shortTermBuckets = activeShortTermBuckets + 5

    String consistency
    Integer threshold
    Integer fetchSize

    ColumnDefinitions getColumnDefinitions() { curBucketRS.columnDefinitions }

    /**
     * Required init call: call post-instantiation once everything is set up in the properties
     * @return
     */

    TimeBucketedSpanningRS init() {
        assert drv: "Cass driver not set"
        assert startSecond: "second not specified to start queries from"
        currentMillis = currentMillis ?: System.currentTimeMillis()
        bucketQueries = this.bucketQueries(startSecond, currentMillis, millisPerBucket, activeShortTermBuckets, shortTermBuckets, tablePrefix)
        assert bucketQueries?.size() > 0: "starting second $startSecond has no active bucket for current time $currentMillis"
        curBucketRS = drv.execSync(bucketQueries[0])
        bucketQueryIndex = 0
        if (threshold != null || fetchSize != null) {
            assert (threshold != null && fetchSize != null): "both threshold and fetchSize must be specified for prefetching to work"
            assert (threshold < fetchSize): "threshold must be less than fetchSize for prefetching to work"
        }
        initialized = true
        return this
    }

    String buildCQL(String tableName) {
        "SELECT * from ${keyspace ? keyspace + '.' : ''}${tableName} WHERE group = ? AND second >= ?"
    }

    // --- resultSet implementation methods

    private boolean initialized = false
    private ResultSet curBucketRS
    private ResultSet nextBucketRS
    private List<St> bucketQueries
    private int bucketQueryIndex = 0

    boolean ready() { initialized }

    Row one() {
        log.trace "TBRS.one(): top"
        if (!initialized) throw new IllegalStateException('TimeBucketedRS not yet initialized')
        fetchMore()
        Row row =  curBucketRS?.one()
        log.trace "TBRS.one(): "
        return row
    }

    boolean isExhausted() {
        log.trace "TBRS.exhausted :: top"
        if (!initialized) throw new IllegalStateException('TimeBucketedRS.exhausted :: not yet initialized')
        boolean exhausted = curBucketRS == null || (curBucketRS.exhausted && bucketQueryIndex >= bucketQueries.size() - 1)
        return exhausted
    }


    boolean fetchMore() {
        log.trace "TBRS.fetchThreshold :: top, check exhausted ${bucketQueryIndex} ${bucketQueries.get(bucketQueryIndex)?.cql}"
        log.trace "                       cur rows avail: ${curBucketRS?.availableWithoutFetching}"
        log.trace "                       nxt rows avail: ${nextBucketRS?.availableWithoutFetching}"

        // check if it's time for the next bucket to become the current bucket
        if (curBucketRS?.exhausted) {
            log.trace "TBRS.fetchThreshold :: cur is exhausted or.. null? ${curBucketRS}"
            // are there more queries in the query list to perform?
            if (bucketQueryIndex < bucketQueries.size() - 1) {
                log.trace "TBRS.fetchThreshold ::   more bucketqueries"
                // is the next query started?
                if (nextBucketRS != null) {
                    log.trace "TBRS.fetchThreshold ::     nextRs ready"
                    // make the next query the current one
                    bucketQueryIndex = bucketQueryIndex + 1
                    curBucketRS = nextBucketRS
                    nextBucketRS = null
                    // ... what if the next bucket is empty? recurse to force another pull
                    if (curBucketRS.exhausted) {
                        log.trace "TBRS.fetchThreshold ::       RECURSING since just-pulled query has no rows"
                        // recurse. we shouldn't have too many tables because of cassandra limits, so stack depth
                        // shouldn't be a danger
                        return fetchMore()
                    }
                    log.trace "TBRS.fetchThreshold :: return true 1, nextRS has become curRS and should have processable rows"
                    return true
                } else {
                    log.trace "TBRS.fetchThreshold ::     nextRs NOT ready - cur is ${bucketQueries[bucketQueryIndex].cql} ${bucketQueries[bucketQueryIndex].args}"
                    bucketQueryIndex = bucketQueryIndex + 1
                    curBucketRS = drv.execSync(bucketQueries[bucketQueryIndex])
                    log.trace "TBRS.fetchThreshold ::     curRS has been queried for $bucketQueryIndex ${bucketQueries.get(bucketQueryIndex)?.cql}"
                    if (curBucketRS.exhausted) {
                        log.trace "TBRS.fetchThreshold ::       RECURSING, just-pulled query has no rows"
                        // recurse. we shouldn't have too many tables because of cassandra limits, so stack depth
                        // shouldn't be a danger
                        return fetchMore()
                    }
                    log.trace "TBRS.fetchThreshold :: return true 2"
                    return true
                }
            } else {
                log.trace "TBRS.fetchThreshold :: no more buckets to retrieve and cur is exhausted, nulling out result sets"
                curBucketRS = null
                nextBucketRS = null
                return false // no more buckets to retrieve
            }
        }
        // current RS is not exhausted, so has processable rows to return. check if we need to start another fetch
        // or trigger fetch of next resultSet
        log.trace "TBRS.fetchThreshold :: PREFETCH"
        if (bucketQueries[bucketQueryIndex].fetchThreshold) {
            log.trace "TBRS.fetchThreshold :: fetchThreshold configured"
            if (curBucketRS) {
                if (curBucketRS.fullyFetched) {
                    log.trace "TBRS.fetchThreshold ::   fully fetched"
                    if (nextBucketRS == null) {
                        log.trace "TBRS.fetchThreshold ::     nextRS not ready"
                        if (bucketQueries[bucketQueryIndex].fetchThreshold >= curBucketRS.availableWithoutFetching) {
                            log.trace "TBRS.fetchThreshold ::       should get nextRS"
                            if (bucketQueryIndex < bucketQueries.size() - 1) {
                                // start pulling the next bucket
                                log.trace "TBRS.fetchThreshold ::         query still todo, start nextRS - exec ${bucketQueries[bucketQueryIndex].cql} ${bucketQueries[bucketQueryIndex].args}"
                                nextBucketRS = drv.execSync(bucketQueries[bucketQueryIndex + 1])
                                return true
                            } else {
                                log.trace "fetchThreshold ::         no more buckets"
                                return false // no more buckets left to prefetch
                            }
                        } else {
                            log.trace " fetchThreshold ::       current rows available not under threshold"
                            return false // current result set still has a lot of rows, wait before starting next bucket
                        }
                    } else {
                        log.trace "TBRS.fetchThreshold ::     nextRS prefetch already initiated"
                        return false // next bucket prefetch pulling has already been started
                    }
                } else {
                    // current RS NOT fully fetched
                    log.trace "TBRS.fetchThreshold ::   more to fetch, see if we are under the fetch Threshold to trigger a fetch"
                    if (bucketQueries[bucketQueryIndex].fetchThreshold >= curBucketRS.availableWithoutFetching && !curBucketRS.fullyFetched) {
                        log.trace "TBRS.fetchThreshold ::     under fetch threshold $threshold, FETCH MORE"
                        curBucketRS.fetchMoreResults()
                        return true
                    } else {
                        log.trace "fetchThreshold ::     plenty remaining ${curBucketRS.availableWithoutFetching}, no fetch triggered"
                        return false  // current set not under fetch next rows threshold
                    }
                }
            } else {
                log.trace("Threshold configured, but no currentRS. likely end of querying")
            }
        } else {
            log.trace "fetchThreshold :: NO THRESHOLD"
            return false // no thresholding specified, so we don't prefetch
        }
    }

    /**
     * produce a list of queries and prepargs to step through across the rotating time buckets. This version in this class
     * only calculates a list for a single class of bucket (short term or long term)
     *
     * @param startSecond - start second (should we just make this Millis so both params are same unit?)
     * @param curMillis - system current time millis
     * @return List < [ cql , cqlarg ] >  - list of tuples
     */
    List<St> bucketQueries(long startSecond, long curMillis, long millisPerBucket, long activeBuckets, long totalBuckets, String tablePrefix) {
        long startMillis = startSecond * 1000L
        long beginOfBucketsMillis = curMillis.intdiv(millisPerBucket) * millisPerBucket

        if (startMillis < beginOfBucketsMillis) {
            startMillis = beginOfBucketsMillis
        }

        long bucketsAhead = (long) ((startMillis).intdiv(millisPerBucket)) - (long) curMillis.intdiv(millisPerBucket)
        if (bucketsAhead > activeBuckets) {
            // too far in the future
            return []
        }
        List<List> longTerm = TimeBucketU.calcBucketList(curMillis, bucketsAhead, tablePrefix, activeBuckets, totalBuckets, millisPerBucket)
        longTerm[0][1] = startMillis
        List<St> queries = []
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

