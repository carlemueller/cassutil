package cass.rs

import cass.drv.Drv
import cass.drv.Rs
import cass.drv.St
import cass.util.RowU
import cass.util.TimeBucketU
import com.datastax.driver.core.ColumnDefinitions
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import javax.xml.ws.Holder

/**
 * This result set is intended to provide query services for a group of sharded tables with an implicit order to the
 * data shards. For example, ****'s scheduled table could be split across time-shards in different tables for easy
 * truncation rather than wrestling with compaction, but we'd still want to be able to query across all the tables
 * as if they were one table.
 *
 * This assumes two tiers of time buckets: a series of short-term small buckets (14 x 12 hr buckets) that "churn" rapidly
 * and a set of longer-term (24 x 30 day) long term buckets. Those assumptions are specific to the ***** app. This
 * version of the class is kind of like migration resultset: it will query from both longterm and shortterm, collating
 * the results, but preferring the shortterm, which should have newer updates with proper update routing if the column
 * keys/execution second matches
 *
 * Short term buckets have more data that appears in them. But long-term data buckets can also have data. Rather than
 * actively transfer data from the long to the short term that TimeBucketedRS relied on, this one integrates and
 * double-queries the long term bucket while short terms are being queried, merging as needed, but preferring short term
 * if there is a primary key / second conflict, since writes prefer short term over long term when the short term
 * bucket becomes active
 *
 * Optionally a "monthly"/30dy basis, the purgable long term bucket should be truncated. Every 12 hrs the purgable short term
 * bucket should be truncated.... It may be technically possible that relying on TTLs we wouldn't need to do any active
 * truncation since the bucketing should reduce the scope of compaction processing
 *
 * With buffer buckets, there should be about 43 tables total.
 *
 * Oh, and there is a fallthrough to the legacy table as well.
 *
 */

// TODO: test: use a comparison with base non-bucketed table with random data generation and this.
// TODO: replace this with Collating(Collating(TimeBucketedSpanning,TimeBucketedSpanning),LegacyRS)

@Slf4j
@CompileStatic
class TimeBucketedCollatingRS implements Rs<Row> {
    Drv drv

    String group
    long startSecond
    long currentMillis

    String keyspace = 'testschema'
    String shortTablePrefix = "scheduled_st_"
    String longTablePrefix = "scheduled_lt_"

    boolean legacyCutover = true

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
    TimeBucketedCollatingRS init() {
        assert drv: "Cass driver not set"
        assert startSecond: "second not specified to start queries from"
        currentMillis = currentMillis ?: System.currentTimeMillis()
        bucketQueries = this.bucketQueries(startSecond, currentMillis, millisPerShort, activeShortTermBuckets, shortTermBuckets, shortTablePrefix)
        bucketQueriesLongTerm = this.bucketQueries(startSecond, currentMillis, millisPerLong, activeLongTermBuckets, longTermBuckets, longTablePrefix)
        assert bucketQueries?.size() > 0 || bucketQueriesLongTerm?.size() > 0: "starting second $startSecond has no active bucket for current time $currentMillis"
        curBucketRS = new Holder<>(drv.execSync(bucketQueries[0]))
        curLongTermRS = new Holder<>(drv.execSync(bucketQueriesLongTerm[0]))
        bucketQueryIndex = new Holder<Integer>(0)
        bucketQueryIndexLongTerm = new Holder<Integer>(0)
        lastRowSource = [true, true, false]
        initialized = true
        if (threshold != null || fetchSize != null) {
            assert (threshold != null && fetchSize != null): "both threshold and fetchSize must be specified if either is set"
            assert (threshold < fetchSize): "threshold must be less than fetchSize"
        }
        if (legacyCutover) {
            lastRowSource[LEGACY] = true
            if (legacyRS == null) {
                legacyRS = new Holder<>(drv.execSync("SELECT * from ${keyspace + '.'}scheduled WHERE second > ${startSecond * 1000L}"))
            }
        }

        return this
    }

    String buildCQL(String tableName) {
        "SELECT * from ${keyspace ? keyspace + '.' : ""}${tableName} WHERE group = ? AND second >= ?"
    }

    // --- resultSet implementation methods

    private static final int LONGTERM = 0
    private static final int SHORTTERM = 1
    private static final int LEGACY = 2

    private boolean initialized = false
    private List<Boolean> lastRowSource
    private Holder<ResultSet> curBucketRS = new Holder<>()
    private Holder<ResultSet> nextBucketRS = new Holder<>()
    private Row curShortRow
    private Holder<ResultSet> curLongTermRS = new Holder<>()
    private Holder<ResultSet> nextLongTermRS = new Holder<>()
    private Row curLongRow
    private List<St> bucketQueries
    private List<St> bucketQueriesLongTerm
    private Holder<Integer> bucketQueryIndex = new Holder<>(0)
    private Holder<Integer> bucketQueryIndexLongTerm = new Holder<>(0)

    private Holder<ResultSet> legacyRS
    private Row curLegacyRow

    ColumnDefinitions getColumnDefinitions() { curBucketRS.value.columnDefinitions}

    boolean ready() { initialized }

    Integer min(Row... vals) {
        if (vals == null) return null
        if (vals.length == 1) return vals[0] == null ? null : 0
        if (vals.length == 2) return compare(vals[0], vals[1]) > 0 ? 1 : 0
        Integer minIdx = vals[0] == null ? null : 0
        for (int i = 1; i < vals.length; i++) {
            if (vals[i] != null) {
                if (minIdx == null) {
                    minIdx = i
                } else {
                    if (vals[minIdx] == null) {
                        minIdx = i
                    }
                    if (compare(vals[i], vals[minIdx]) < 0) {
                        minIdx = i
                    }
                }
            }
        }
        return minIdx
    }

    void resetRowSource() {
        lastRowSource[SHORTTERM] = false
        lastRowSource[LONGTERM] = false
        lastRowSource[LEGACY] = false
    }

    int compare(Row r1, Row r2) {
        r1.getLong(1) <=> r2.getLong(1) ?:
                r1.getString(2) <=> r2.getString(2) ?:
                        r1.getString(3) <=> r2.getString(3)
    }

    // basically the same as CollatingResultSet
    Row one() {
        log.trace "TBRS.one(): top"
        if (!initialized) throw new IllegalStateException('TimeBucketedRS not yet initialized')
        fetchMore()
        log.trace("TBRS.one(): BEF st[${curShortRow?.getLong(1)}] lt[${curLongRow?.getLong(1)}] leg[${curShortRow?.getLong(1)}]")
        if (lastRowSource[SHORTTERM]) {
            log.trace("TBRS.one(): pull another short row")
            curShortRow = curBucketRS.value?.one()
        }
        if (lastRowSource[LONGTERM]) {
            log.trace("TBRS.one(): pull another long row")
            curLongRow = curLongTermRS.value?.one()
        }
        if (lastRowSource[LEGACY]) {
            log.trace("TBRS.one(): pull another legacy row")
            curLegacyRow = legacyRS.value?.one()
        }
        log.trace("TBRS.one(): AFT st[${curShortRow?.getLong(1)}] lt[${curLongRow?.getLong(1)}] leg[${curShortRow?.getLong(1)}]")
        resetRowSource()
        if (curShortRow == null && curLongRow == null && curLegacyRow == null) {
            log.trace("TBRS.one(): all current rows null")
            return null
        }

        Row[] rows = [curShortRow, curLongRow, legacyCutover ? curLegacyRow : null] as Row[]
        Integer minIdx = min(rows)
        Row rowToReturn = minIdx == null ? null : rows[minIdx]
        log.trace("TBRS.one() row idx ${minIdx} for ${RowU.toList(rowToReturn)}")
        lastRowSource[SHORTTERM] = curShortRow == null ? true : compare(rowToReturn, curShortRow) == 0
        lastRowSource[LONGTERM] = curLongRow == null ? true : compare(rowToReturn, curLongRow) == 0
        lastRowSource[LEGACY] = legacyCutover ? curLegacyRow == null ? true : compare(rowToReturn, curLegacyRow) == 0 : false
        return rowToReturn
    }

    boolean fetchMore() {
        fetchMore(curBucketRS, nextBucketRS, bucketQueries, bucketQueryIndex) ||
                fetchMore(curLongTermRS, nextLongTermRS, bucketQueriesLongTerm, bucketQueryIndexLongTerm) ||
                legacyCutover ? fetchMoreLegacy() : false
    }

    boolean fetchMoreLegacy() {
        if (threshold != null && legacyRS?.value != null && !legacyRS.value.fullyFetched && !legacyRS.value.exhausted && legacyRS.value.availableWithoutFetching <= threshold) {
            legacyRS.value.fetchMoreResults()
            return true
        }
        return false
    }

    /**
     * handle all cases:
     * - no threshold specified (disables nextBucket prefetching, next bucket is only )
     * - fetching next set of data in current bucket
     * - current bucket is done pulling rows, so start pulling the next bucket if possible
     *
     * @return
     */
    boolean fetchMore(Holder<ResultSet> curBucketRS, Holder<ResultSet> nextBucketRS, List<St> bucketQueries, Holder<Integer> bucketQueryIndex) {
        log.trace "TBRS.fetchThreshold :: top, check exhausted ${bucketQueryIndex.value} ${bucketQueries.get(bucketQueryIndex.value)?.cql}"
        log.trace "                       cur rows avail: ${curBucketRS.value?.availableWithoutFetching}"
        log.trace "                       nxt rows avail: ${nextBucketRS.value?.availableWithoutFetching}"

        // check if it's time for the next bucket to become the current bucket
        if (curBucketRS.value?.exhausted) {
            log.trace "TBRS.fetchThreshold :: cur is exhausted or.. null? ${curBucketRS.value}"
            // are there more queries in the query list to perform?
            if (bucketQueryIndex.value < bucketQueries.size() - 1) {
                log.trace "TBRS.fetchThreshold ::   more bucketqueries"
                // is the next query started?
                if (nextBucketRS.value != null) {
                    log.trace "TBRS.fetchThreshold ::     nextRs ready"
                    // make the next query the current one
                    bucketQueryIndex.value = bucketQueryIndex.value + 1
                    curBucketRS.value = nextBucketRS.value
                    nextBucketRS.value = null
                    // ... what if the next bucket is empty? recurse to force another pull
                    if (curBucketRS.value.exhausted) {
                        log.trace "TBRS.fetchThreshold ::       RECURSING since just-pulled query has no rows"
                        // recurse. we shouldn't have too many tables because of cassandra limits, so stack depth
                        // shouldn't be a danger
                        return fetchMore(curBucketRS, nextBucketRS, bucketQueries, bucketQueryIndex)
                    }
                    log.trace "TBRS.fetchThreshold :: return true 1, nextRS has become curRS and should have processable rows"
                    return true
                } else {
                    log.trace "TBRS.fetchThreshold ::     nextRs NOT ready - cur is ${bucketQueries[bucketQueryIndex.value].cql} ${bucketQueries[bucketQueryIndex.value].args}"
                    bucketQueryIndex.value = bucketQueryIndex.value + 1
                    curBucketRS.value = drv.execSync(bucketQueries[bucketQueryIndex.value])
                    log.trace "TBRS.fetchThreshold ::     curRS has been queried for $bucketQueryIndex ${bucketQueries.get(bucketQueryIndex.value)?.cql}"
                    if (curBucketRS.value.exhausted) {
                        log.trace "TBRS.fetchThreshold ::       RECURSING, just-pulled query has no rows"
                        // recurse. we shouldn't have too many tables because of cassandra limits, so stack depth
                        // shouldn't be a danger
                        return fetchMore(curBucketRS, nextBucketRS, bucketQueries, bucketQueryIndex)
                    }
                    log.trace "TBRS.fetchThreshold :: return true 2"
                    return true
                }
            } else {
                log.trace "TBRS.fetchThreshold :: no more buckets to retrieve and cur is exhausted, nulling out result sets"
                curBucketRS.value = null
                nextBucketRS.value = null
                return false // no more buckets to retrieve
            }
        }
        // current RS is not exhausted, so has processable rows to return. check if we need to start another fetch
        // or trigger fetch of next resultSet
        log.trace "TBRS.fetchThreshold :: PREFETCH"
        if (bucketQueries[bucketQueryIndex.value].fetchThreshold) {
            log.trace "TBRS.fetchThreshold :: fetchThreshold configured"
            if (curBucketRS.value) {
                if (curBucketRS.value.fullyFetched) {
                    log.trace "TBRS.fetchThreshold ::   fully fetched"
                    if (nextBucketRS.value == null) {
                        log.trace "TBRS.fetchThreshold ::     nextRS not ready"
                        if (bucketQueries[bucketQueryIndex.value].fetchThreshold >= curBucketRS.value.availableWithoutFetching) {
                            log.trace "TBRS.fetchThreshold ::       should get nextRS"
                            if (bucketQueryIndex.value < bucketQueries.size() - 1) {
                                // start pulling the next bucket
                                log.trace "TBRS.fetchThreshold ::         query still todo, start nextRS - exec ${bucketQueries[bucketQueryIndex.value].cql} ${bucketQueries[bucketQueryIndex.value].args}"
                                nextBucketRS.value = drv.execSync(bucketQueries[bucketQueryIndex.value + 1])
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
                    if (bucketQueries[bucketQueryIndex.value].fetchThreshold >= curBucketRS.value.availableWithoutFetching && !curBucketRS.value.fullyFetched) {
                        log.trace "TBRS.fetchThreshold ::     under fetch threshold $threshold, FETCH MORE"
                        curBucketRS.value.fetchMoreResults()
                        return true
                    } else {
                        log.trace "fetchThreshold ::     plenty remaining ${curBucketRS.value.availableWithoutFetching}, no fetch triggered"
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

    boolean isExhausted() {
        log.trace "TBRS.exhausted :: top"
        if (!initialized) throw new IllegalStateException('TimeBucketedRS.exhausted :: not yet initialized')
        boolean shortTermExhausted = curBucketRS.value == null || (curBucketRS.value.exhausted && bucketQueryIndex.value == bucketQueries.size() - 1)
        boolean longTermExhausted = curLongTermRS.value == null || (curLongTermRS.value.exhausted && bucketQueryIndexLongTerm.value == bucketQueriesLongTerm.size() - 1)
        boolean exhausted = shortTermExhausted && longTermExhausted
        log.trace "TBRS.exhausted :: st $shortTermExhausted lt $longTermExhausted overall $exhausted"
        return exhausted
    }

    // ---- helper logic



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


    String calcStartSecondBucketTable() {
        // see if this is within the short term period
        String shortTable = TimeBucketU.calcBucketTable(startSecond * 1000L, currentMillis, millisPerShort, shortTermBuckets, activeShortTermBuckets, shortTablePrefix)
        if (shortTable != null) {
            return shortTable
        }
        return TimeBucketU.calcBucketTable(startSecond * 1000L, currentMillis, millisPerLong, longTermBuckets, activeLongTermBuckets, longTablePrefix)
    }

}

