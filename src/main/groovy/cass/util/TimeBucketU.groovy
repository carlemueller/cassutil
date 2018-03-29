package cass.util

class TimeBucketU {

    /**
     * Calculate the inclusive STARTING millisecond of a bucket for the given millisecond event
     *
     * Does not enforce range limits or grace
     *
     * @param millis
     * @param millisPerBucket
     * @return
     */
    static long bucketBegin(long millis, long millisPerBucket) {
        (long) millis.intdiv(millisPerBucket) * millisPerBucket
    }

    /**
     * Calculate the inclusive ENDING millisecond of a bucket for the given millisecond event
     *
     * Does not enforce range limits or grace
     *
     * @param millis
     * @param millisPerBucket
     * @return
     */
    static long bucketEnd(long millis, long millisPerBucket) {
        bucketBegin(millis, millisPerBucket) + millisPerBucket - 1L
    }

    /**
     * Calculate how many buckets away from "curMillis" millis is. Negative means we are into grace buckets / nonactive buckets
     *
     * This does not enforce range limits on buckets with respect to active / grace buckets.
     *
     * @param millis
     * @param curMillis
     * @param millisPerBucket
     * @return
     */
    static long bucketOffset(long millis, long curMillis, long millisPerBucket) {
        (long) millis.intdiv(millisPerBucket) - (long) curMillis.intdiv(millisPerBucket)
    }

    /**
     * This just calculates the inclusive start and end point of the "modulus" time bucket, it does not check against curMillis
     * to see if the millis are in an active bucket or not.
     *
     * @param millis
     * @param millisPerBucket
     * @param totalBuckets
     * @return
     */
    static long calcBucket(long millis, long millisPerBucket, long totalBuckets) {
        (long) millis.intdiv(millisPerBucket) % totalBuckets
    }

    /**
     * Calculates the target table for a class of bucket (i.e. long or short), enforcing range limits for grace and
     * active buckets.
     *
     * @param startMillis
     * @param currentMillis
     * @param millisPerBucket
     * @param buckets
     * @param activeBuckets
     * @param tablePrefix
     * @return the table name to update, or null if out of active/grace enforcement
     */
    static String calcBucketTable(long startMillis, long currentMillis, long millisPerBucket, long buckets, long activeBuckets, String tablePrefix) {
        long offset = bucketOffset(startMillis, currentMillis, millisPerBucket)
        if (offset < 0) {
            // this assumes a single grace bucket
            if (offset < -1 * (buckets - activeBuckets).intdiv(2)) {
                return null
            }
            return tablePrefix + calcBucket(startMillis, millisPerBucket, buckets)
        } else if (offset >= activeBuckets) {
            // bucket is too far in the future and may/will loop into grace and garbage collecting buckets
            return null
        } else {
            return tablePrefix + calcBucket(startMillis, millisPerBucket, buckets)
        }
    }

    static long[] bucketStartEndMillis(long millis, long millisPerBucket) {
        [bucketBegin(millis,millisPerBucket), bucketEnd(millis,millisPerBucket)] as long[]
    }

    /**
     * Calculate buckets starting from the indicated current active bucket. This is the same process currently for
     * both short and long term buckets, all that differs is the input values.
     *
     * This is used to generate a list of queries that will be cycled through to span across time buckets, for example
     * in the time bucketed scheduled table
     *
     * @param firstBucketMillis
     * @param startBucket
     * @param tablePrefix
     * @param activeBuckets
     * @param totalBuckets
     * @param millisPerBucket
     * @return
     */
    static List<List> calcBucketList(long firstBucketMillis, long startBucket, String tablePrefix, long activeBuckets, long totalBuckets, long millisPerBucket) {
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


}
