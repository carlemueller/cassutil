package helpers

class TestSchema {

    // this is the maximum number of inserts that will be performed by a thread per second. Note it may not reach this under load.
    static int throttleMaxInsertsPerThread = 10000

    static String keyspace = "testschema"

    static List<String> dockerIPs = ["127.0.0.1"] // local single non-docker
    //static List<String> dockerIPs = ["172.17.0.2", "172.17.0.3", "172.17.0.4"] // docker three-node cluster

    static int defaultReplicationFactor = 1

    static String CREATE_KS(Integer rf, String ks) {
        "CREATE KEYSPACE ${ks ?:keyspace} WITH replication = {'class':'SimpleStrategy','replication_factor':${rf ?: defaultReplicationFactor}};"
    }

    static String CREATE_scheduled(String timeShard, String ks) {
        """
            CREATE TABLE ${ks ?:keyspace}.scheduled${timeShard ?: ''} (
                group text,
                second bigint,
                name text,
                jobtype text,
                data text,
                expression text,
                runtime bigint,
                status text,
                timezone text,
                PRIMARY KEY (group, second, name, jobtype)
            ) WITH CLUSTERING ORDER BY (second ASC, name ASC, jobtype ASC)
                AND bloom_filter_fp_chance = 0.01
                AND comment = ''
                AND compaction = {'tombstone_threshold': '0.2', 'tombstone_compaction_interval': '3600', 'unchecked_tombstone_compaction': 'true', 'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
                AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
                AND dclocal_read_repair_chance = 0.1
                AND default_time_to_live = 0
                AND gc_grace_seconds = 10800
                AND max_index_interval = 2048
                AND memtable_flush_period_in_ms = 0
                AND min_index_interval = 128
                AND read_repair_chance = 0.0
                AND speculative_retry = '99.0PERCENTILE';
        """
    }

    static String CREATE_second_schedules(String timeShard, String ks) {
        """
            CREATE TABLE ${ks ?:keyspace}.second_schedules${timeShard ?: ''} (
                second bigint,
                group text,
                name text,
                jobtype text,
                data text,
                expression text,
                runtime bigint,
                status text,
                timezone text,
                PRIMARY KEY (second, group, name, jobtype)
            ) WITH CLUSTERING ORDER BY (group ASC, name ASC, jobtype ASC)
                AND bloom_filter_fp_chance = 0.01
                AND comment = ''
                AND compaction = {'tombstone_threshold': '0.2', 'tombstone_compaction_interval': '3600', 'unchecked_tombstone_compaction': 'true', 'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
                AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
                AND dclocal_read_repair_chance = 0.1
                AND default_time_to_live = 0
                AND gc_grace_seconds = 10800
                AND max_index_interval = 2048
                AND memtable_flush_period_in_ms = 0
                AND min_index_interval = 128
                AND read_repair_chance = 0.0
                AND speculative_retry = '99.0PERCENTILE';
        """
    }

    static String insertSecondSchedules(String shard, String ks) {
        "INSERT INTO ${ks ?:keyspace}.second_schedules${shard ?: ''} (second,group,name,jobtype,data) VALUES (?,?,?,?,?);"
    }

    static String insertSecondSchedulesAllCols(String shard, String ks) {
        "INSERT INTO ${ks ?:keyspace}.second_schedules${shard ?: ''} (second,group,name,jobtype,data,expression,runtime,status,timezone) VALUES (?,?,?,?,?,?,?,?,?);"
    }

    static String insertScheduled(String shard, String ks) {
        "INSERT INTO ${ks ?:keyspace}.scheduled${shard ?: ''} (group,second,name,jobtype,data) VALUES (?,?,?,?,?);"
    }

    static String insertScheduledAllCols(String shard, String ks) {
        "INSERT INTO ${ks ?:keyspace}.scheduled${shard ?: ''} (group,second,name,jobtype,data,expression,runtime,status,timezone) VALUES (?,?,?,?,?,?,?,?,?);"
    }

    static String ttlInsertSecondSchedules(String shard, String ks) {
        "INSERT INTO ${ks ?:keyspace}.second_schedules${shard ?: ''} (second,group,name,jobtype,data) VALUES (?,?,?,?,?) USING TTL ? ;"
    }

    static String ttlInsertScheduled(String shard, String ks) {
        "INSERT INTO ${ks ?:keyspace}.scheduled${shard ?: ''} (group,second,name,jobtype,data) VALUES (?,?,?,?,?) USING TTL ?;"
    }

    static String selectSecondSchedules(String shard, String ks) {
        "SELECT group, name, jobtype, data FROM ${ks ?:keyspace}.second_schedules${shard ?: ''} WHERE second = ?"
    }

    static String selectScheduledForGroupAfterSecond(String shard, String ks) {
        "SELECT group, second, name, jobtype, data FROM ${ks ?:keyspace}.scheduled${shard ?: ''} WHERE group = ? AND second >= ?"
    }

}

