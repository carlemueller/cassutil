package cass.rs

import cass.drv.St
import spock.lang.Specification
import spock.lang.Unroll

class TimeBucketingSpec extends Specification{

    @Unroll
    void 'Test basic query list generation for #reason'() {
        given:

        TimeBucketedRS rs = new TimeBucketedRS(startSecond: querySecond)

        List<Long> expectedStBucketMillis = (0..rs.shortTermBuckets).collect {
            (long) (currentTimeMillis.intdiv(rs.millisPerShort) * rs.millisPerShort) + (long) (it * rs.millisPerShort)
        }
        List<Long> expectedLtBucketMillis = (0..rs.longTermBuckets).collect {
            (long) ((currentTimeMillis + rs.shortTermBuckets * (long) rs.activeShortTermBuckets).intdiv(rs.millisPerLong) * rs.millisPerLong) + (long) (it * rs.millisPerLong)
        }

        when: 'we do a query a bit in the future of "current time" '
        List<St> queries = rs.bucketQueries(querySecond, currentTimeMillis)
        long shortTermBucketSinceEpoch = currentTimeMillis.intdiv(rs.millisPerShort)
        long shortTermBucketRotating = shortTermBucketSinceEpoch % rs.shortTermBuckets

        then: 'get expected normal'
        println "-------- $reason --------"
        if (queries) {
            queries.each {
                print it.cql
                print " -- " + it.args[1]
                print " -- " + it.args[1].intdiv(rs.millisPerShort)
                println " -- " + it.args[1].intdiv(rs.millisPerLong)
            }
        }

        // verify number of queries/active buckets
        queries.size() == qrySz

        // verify the first bucket is properly bucketed if we queried from current bucket
        if (qrySz == 38) {
            queries[0].cql.contains("st_${shortTermBucketRotating}")
            queries[0].args[1] >= rs.millisPerShort * shortTermBucketSinceEpoch
            queries[0].args[1] < rs.millisPerShort * (shortTermBucketSinceEpoch + 1L)

            // verify all st buckets are of the correct lower bound
            rs.activeShortTermBuckets.times { int i -> assert i == 0 ? queries[i].args[1] >= expectedStBucketMillis[i] : queries[i].args[1] == expectedStBucketMillis[i] }
            rs.activeLongTermBuckets.times { int i -> assert i == 0 ? queries[i + rs.activeShortTermBuckets].args[1] >= expectedLtBucketMillis[i] : queries[i + rs.activeShortTermBuckets].args[1] == expectedLtBucketMillis[i] }
        }

        where:
        currentTimeMillis  | querySecond    | qrySz | reason
        1_500_000_000_000L | 1_500_000_002L | 38    | 'a nice even starting point in 2017'
        1_500_000_000_000L | 1_499_999_998L | 38    | 'query a couple seconds in the past but not in grace bucket'
        1_500_000_000_000L | 1_499_990_001L | 39    | 'query that is in the grace bucket'
        1_500_000_000_000L | 1_400_000_001L | 39    | 'query that is before the grace bucket'
        1_500_000_000_000L | 1_500_550_001L | 26    | 'query that is just inside of the last two short term buckets'
        1_500_000_000_000L | 1_500_552_001L | 25    | 'query that is just inside of the last short term bucket'
        1_500_000_000_000L | 1_500_597_001L | 24    | 'query that is just outside of the short term buckets'
        1_500_000_000_000L | 1_600_000_000L | 0     | 'query that is past the long term max bucket'
    }
}
