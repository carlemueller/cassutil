package cass.rs

import cass.drv.Drv
import cass.util.RowU
import cass.util.TimeBucketU
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.fasterxml.jackson.core.type.TypeReference
import com.google.common.collect.Lists
import helpers.LogUtil
import helpers.TestSchema
import jsonutil.JSONUtil
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Stepwise
import spock.lang.Unroll

import javax.xml.ws.Holder

@Stepwise
class TimeBucketedCollatingRSSpec extends Specification {

    @Shared
    static Drv drv

    static String keyspace = 'testschema'

    boolean verbose = false

    def setupSpec() {
        LogUtil.setLogLevel('org.apache', "ERROR")
        LogUtil.setLogLevel('io.netty', "ERROR")
        LogUtil.setLogLevel('com.datastax', "ERROR")
        LogUtil.setLogLevel('cass.drv', "ERROR")
        EmbeddedCassandraServerHelper.startEmbeddedCassandra()
        println 'testdir: ' + new File('./newfile').getAbsolutePath()
    }

    def resetAndSchema(int stBuckets, int ltBuckets) {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
        drv = new Drv(autoStart: true).nodes("127.0.0.1").port(9142)
        try {
            drv.execSync(TestSchema.CREATE_KS(1, keyspace), null)
        } catch (Exception e) {
        }
        try {
            stBuckets.times {
                drv.execSync(TestSchema.CREATE_scheduled('_st_' + it, keyspace), null)
            }
        } catch (Exception e) {
        }
        try {
            ltBuckets.times {
                drv.execSync(TestSchema.CREATE_scheduled('_lt_' + it, keyspace), null)
            }
        } catch (Exception e) {
        }

        try {
            drv.execSync(TestSchema.CREATE_scheduled('_legacy', keyspace), null)
            drv.execSync(TestSchema.CREATE_scheduled('_baseline', keyspace), null)
        } catch (Exception e) {
        }

    }

    @Unroll
    void 'test various bucket lengths and start points #desc'() {
        given:
        Random rnd = new Random(8921389012809)
        resetAndSchema(stb, ltb)
        TimeBucketedCollatingRS rs = new TimeBucketedCollatingRS(
                keyspace: keyspace,
                group: 'group1',
                activeShortTermBuckets: actSTB,
                shortTermBuckets: stb,
                activeLongTermBuckets: actLTB,
                longTermBuckets: ltb,
                millisPerShort: msST,
                millisPerLong: msLT,
                threshold: thresh,
                fetchSize: fetch,
                drv: drv,
                // more configuration below
        )

        // generate a current Millis
        long curMillis = 13L * 365L * 24L * 3600L * 1000L // 13 years after epoch
        long startMillis = (long) ((curMillis + startMSOffset).intdiv(1000)) * 1000L
        long maxActiveLongMillis = (long) (curMillis.intdiv(rs.millisPerLong) + rs.activeLongTermBuckets) * rs.millisPerLong - 1L

        Set<Long> usedMillis = [] as Set
        Set<Long> dupeMillis = [] as Set
        rand.times { rnd.nextInt() }
        generateData(rs, genRows, rnd, curMillis, usedMillis, dupeMillis)

        if (verbose) {
            println "----- short -----"
            stb.times {
                Lists.newArrayList(drv.execSync("SELECT * from ${keyspace}.scheduled_st_${it}").iterator()).collect { Row r -> RowU.toList(r) }.each { List l -> println it + JSONUtil.toJSONPretty(l) }
            }
            println "----- long -----"
            ltb.times {
                Lists.newArrayList(drv.execSync("SELECT * from ${keyspace}.scheduled_lt_${it}").iterator()).collect { Row r -> RowU.toList(r) }.each { List l -> println it + JSONUtil.toJSONPretty(l) }
            }
            println "----- legacy -----"
            Lists.newArrayList(drv.execSync("SELECT * from ${keyspace}.scheduled_legacy").iterator()).collect {
                RowU.toList(it)
            }.each { println JSONUtil.toJSONPretty(it) }
            println "----- baseline -----"
            Lists.newArrayList(drv.execSync("SELECT * from ${keyspace}.scheduled_baseline").iterator()).collect {
                RowU.toList(it)
            }.each { println JSONUtil.toJSONPretty(it) }
        }

        // now filter the rows that are superseded and then we should have the expected data

        when: 'we query for both baseline and the collating'
        if (verbose) {
            println "CURRENT ms $curMillis buckets: ST ${curMillis.intdiv(rs.millisPerShort)} LT ${curMillis.intdiv(rs.millisPerLong)}"
            println "  ST-O ${curMillis.intdiv(rs.millisPerShort) % rs.shortTermBuckets}"
            println "  LT-O ${curMillis.intdiv(rs.millisPerLong) % rs.longTermBuckets}"
            println "START ms $startMillis buckets: ST ${startMillis.intdiv(rs.millisPerShort)} LT ${startMillis.intdiv(rs.millisPerLong)}"
            println "  ST-O ${startMillis.intdiv(rs.millisPerShort) % rs.shortTermBuckets}"
            println "  LT-O ${startMillis.intdiv(rs.millisPerLong) % rs.longTermBuckets}"
        }

        rs.startSecond = (long) startMillis.intdiv(1000)
        rs.currentMillis = curMillis
        // setup the legacyRS
        rs.legacyRS = new Holder<ResultSet>(
                drv.execSync(
                        "SELECT * FROM ${keyspace}.scheduled_legacy WHERE group = ? AND second >= ?",
                        ['group1', startMillis] as Object[]
                )
        )
        rs.init()
        // get comparison baseling resultset
        ResultSet baselineRS = drv.execSync(
                "SELECT * FROM ${keyspace}.scheduled_baseline WHERE group = ? AND second >= ?",
                ['group1', startMillis] as Object[]
        )

        if (verbose) {
            LogUtil.setLogLevel('cass.rs', 'TRACE')
        }

        boolean same = true
        int rowCount = 0
        println "MAX ACTIVE: $maxActiveLongMillis"
        while (true) {
            List rowBaseline = RowU.toList(baselineRS.one())
            List rowCollated = RowU.toList(rs.one())
            if (rowBaseline != null && rowBaseline[1] > maxActiveLongMillis) {
                println "excluding baseline ${rowBaseline[1]}, greater than $maxActiveLongMillis"
                rowBaseline = null
            }
            if (rowCollated != rowBaseline) {
                println "MISMATCH $rowCount"
                println "   baseline: " + JSONUtil.serialize(rowBaseline)
                println "   collated: " + JSONUtil.serialize(rowCollated)
                println "     last rowsource: ${rs.lastRowSource}"
                println "     stQ: ${rs.bucketQueries[rs.bucketQueryIndex.value].cql}"
                println "     ltQ: ${rs.bucketQueriesLongTerm[rs.bucketQueryIndexLongTerm.value].cql}"
                println "     STrow: ${rs.curShortRow}"
                println "     LTrow: ${rs.curLongRow}"
                println "     LGrow: ${rs.curLegacyRow}"
                println "     in dupe? ${dupeMillis.contains(rowBaseline[1])}"
                println rs.bucketQueries[rs.bucketQueryIndex.value].cql.replace(">=", "=")
                println "     shortrows: " + drv.execSync(
                        rs.bucketQueries[rs.bucketQueryIndex.value].cql.replace(">=", "="),
                        rs.bucketQueries[rs.bucketQueryIndex.value].args).all()
                println rs.bucketQueriesLongTerm[rs.bucketQueryIndexLongTerm.value].cql.replace(">=", "=")
                println "     longrows: " + drv.execSync(
                        rs.bucketQueriesLongTerm[rs.bucketQueryIndexLongTerm.value].cql.replace(">=", "="),
                        rs.bucketQueriesLongTerm[rs.bucketQueryIndexLongTerm.value].args).all()
                same = false
                break
            } else {
                if (verbose) {
                    println "matched row #${rowCount}"
                    println "   baseline: " + JSONUtil.serialize(rowBaseline)
                    println "   collated: " + JSONUtil.serialize(rowCollated)
                }
            }
            if (rowBaseline == null || rowCollated == null) {
                break
            }
            rowCount++
        }

        if (verbose) {
            println "curMillis: $curMillis   startMillis: ${rs.startSecond * 1000L} $maxActiveLongMillis"
            println "  short bucket: since epoch: ${curMillis.intdiv(rs.millisPerShort)}"
            rs.activeShortTermBuckets.times {
                long bucketMillis = curMillis + (long) it * rs.millisPerShort
                boolean startBucket = rs.startSecond * 1000L >= TimeBucketedCollatingRS.bucketBegin(bucketMillis, rs.millisPerShort) &&
                        rs.startSecond * 1000L <= TimeBucketU.bucketEnd(bucketMillis, rs.millisPerShort)
                println "    bucket $it: aka ${TimeBucketU.calcBucket(bucketMillis, rs.millisPerShort, rs.shortTermBuckets)}" +
                        "${startBucket ? ' START' : ''}" +
                        " begin ${TimeBucketU.bucketBegin(bucketMillis, rs.millisPerShort)}" +
                        " end ${TimeBucketU.bucketEnd(bucketMillis, rs.millisPerShort)}"
            }
            println "  long bucket since epoch: ${curMillis.intdiv(rs.millisPerLong)}"
            rs.activeLongTermBuckets.times {
                long bucketMillis = curMillis + (long) it * rs.millisPerLong
                boolean startBucket = rs.startSecond * 1000L >= TimeBucketedCollatingRS.bucketBegin(bucketMillis, rs.millisPerLong) &&
                        rs.startSecond * 1000L <= TimeBucketU.bucketEnd(bucketMillis, rs.millisPerLong)
                println "    bucket $it: aka ${TimeBucketU.calcBucket(bucketMillis, rs.millisPerLong, rs.longTermBuckets)}" +
                        "${startBucket ? ' START' : ''}" +
                        " begin ${TimeBucketU.bucketBegin(bucketMillis, rs.millisPerLong)}" +
                        " end ${TimeBucketU.bucketEnd(bucketMillis, rs.millisPerLong)}"
            }
        }

        then: 'baseline and collating should match'
        noExceptionThrown()
        same == true

        where:
        msST     | stb | actSTB | msLT        | ltb | actLTB | genRows | rand | thresh | fetch | startMSOffset | desc
        1121121L | 12  | 7      | 2002001L    | 7   | 4      | 11000   | 1000 | 22     | 33    | 1121121L * 2L | "nonaligning buckets and st/lt border overlap"
        //100_000L | 10  | 5      | 10_000_000L | 10  | 7      | 311257  | 111  | 5      | 10    | 58000L        | "300000 test"
    }

    TimeBucketedCollatingRS rs = new TimeBucketedCollatingRS(
            keyspace: keyspace,
            group: 'group1',
            activeShortTermBuckets: 5,
            shortTermBuckets: 10,
            activeLongTermBuckets: 7,
            longTermBuckets: 10,
            millisPerShort: 100_000L,
            millisPerLong: 10_000_000L,
            threshold: 5,
            fetchSize: 10,
            drv: drv,
            // more configuration below
    )

    def cleanupSpec() {
        try {
            drv.destroy()
        } catch (Exception e) {
            println "cleanup error: $e.message"
        }
    }

    void generateData(TimeBucketedCollatingRS rs, int sampleCount, Random rnd, long curMillis, Set usedMillis, Set dupeMillis) {
        sampleCount.times {
            String group = 'group' + rnd.nextInt(3)
            String name = 'name' + rnd.nextInt(10)
            String jobtype = 'jobtype' + rnd.nextInt(10)
            int rand = rnd.nextInt(100)
            long jobMillis = -1L
            if (rand < 70) {
                // short term job
                jobMillis = curMillis + (long) rnd.nextLong() % (rs.activeShortTermBuckets * rs.millisPerShort)
                if (jobMillis in usedMillis) {
                    dupeMillis.add(jobMillis)
                }
                usedMillis.add(jobMillis)
                Integer shortBucket = calculateBucket(jobMillis, curMillis, rs.millisPerShort, rs.activeShortTermBuckets, rs.shortTermBuckets)
                if (shortBucket != null) {
                    drv.execSync(
                            TestSchema.insertScheduledAllCols("_st_${shortBucket}", keyspace),
                            [group, jobMillis, name, jobtype, 'shortdata', 'expr', 1L, 'good', 'UTC'] as Object[])
                    drv.execSync(
                            TestSchema.insertScheduledAllCols("_baseline", keyspace),
                            [group, jobMillis, name, jobtype, 'shortdata', 'expr', 1L, 'good', 'UTC'] as Object[])
                    // 1% of the time, have a conflict in both long and short that short should "win"
                    if (rnd.nextInt(100) < 1) {
                        Integer longBucket = calculateBucket(jobMillis, curMillis, rs.millisPerLong, rs.activeLongTermBuckets, rs.longTermBuckets)
                        drv.execSync(
                                TestSchema.insertScheduledAllCols("_lt_${longBucket}", keyspace),
                                [group, jobMillis, name, jobtype, 'longdata', 'expr', 1L, 'good', 'UTC'] as Object[])
                    }
                }
            } else if (rand < 95) {
                // long term job
                jobMillis = curMillis + (long) rnd.nextLong() % (rs.activeLongTermBuckets * rs.millisPerLong)
                if (jobMillis in usedMillis) {
                    dupeMillis.add(jobMillis)
                }
                usedMillis.add(jobMillis)
                Integer longBucket = calculateBucket(jobMillis, curMillis, rs.millisPerLong, rs.activeLongTermBuckets, rs.longTermBuckets)
                if (longBucket != null) {
                    drv.execSync(
                            TestSchema.insertScheduledAllCols("_lt_${longBucket}", keyspace),
                            [group, jobMillis, name, jobtype, 'longdata', 'expr', 1L, 'good', 'UTC'] as Object[])
                    drv.execSync(
                            TestSchema.insertScheduledAllCols("_baseline", keyspace),
                            [group, jobMillis, name, jobtype, 'longdata', 'expr', 1L, 'good', 'UTC'] as Object[])
                }
            } else {
                // legacy job
                jobMillis = curMillis + (long) rnd.nextLong() % (rs.activeLongTermBuckets * rs.millisPerLong)
                usedMillis.add(jobMillis)

                if (jobMillis in usedMillis) {
                    dupeMillis.add(jobMillis)
                }
                if (jobMillis < TimeBucketU.bucketBegin(curMillis, rs.millisPerLong) + rs.activeLongTermBuckets * rs.millisPerLong - 1L)
                    drv.execSync(
                            TestSchema.insertScheduledAllCols("_legacy", keyspace),
                            [group, jobMillis, name, jobtype, 'legacy', 'expr', 1L, 'good', 'UTC'] as Object[])
                drv.execSync(
                        TestSchema.insertScheduledAllCols("_baseline", keyspace),
                        [group, jobMillis, name, jobtype, 'legacy', 'expr', 1L, 'good', 'UTC'] as Object[])
            }
        } // end test data generation
    }

    static Integer calculateBucket(long millis, long curMillis, long millisPerBucket, long activeBuckets, long totalBuckets) {
        if (TimeBucketU.bucketOffset(millis, curMillis, millisPerBucket) >= activeBuckets) {
            return null
        }
        return (int) TimeBucketU.calcBucket(millis, millisPerBucket, totalBuckets)
    }


    static TypeReference<List<List>> listOfLists() { new TypeReference<List<List>>() {} }
}
