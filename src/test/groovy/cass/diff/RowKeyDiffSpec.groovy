package cass.diff

import cass.drv.Drv
import com.google.common.collect.Lists
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import spock.lang.Shared
import spock.lang.Specification
import tickerproto.LogUtil
import tickerproto.TickerSchema

class RowKeyDiffSpec extends Specification {

    @Shared
    static Drv drv

    static String keyspace = 'tickertest'

    def setupSpec() {
        LogUtil.setLogLevel('org.apache', "ERROR")
        LogUtil.setLogLevel('io.netty', "ERROR")
        LogUtil.setLogLevel('com.datastax', "ERROR")
        EmbeddedCassandraServerHelper.startEmbeddedCassandra()

        drv = new Drv(autoStart: true).nodes("127.0.0.1").port(9142)
        try {
            drv.execSync(TickerSchema.CREATE_KS(1, keyspace), null)
        } catch (Exception e) {
        }
        try {
            drv.execSync(TickerSchema.CREATE_second_schedules('OLD', keyspace), null)
        } catch (Exception e) {
        }
        try {
            drv.execSync(TickerSchema.CREATE_second_schedules('NEW', keyspace), null)
        } catch (Exception e) {
        }

        println 'testdir: ' + new File('./newfile').getAbsolutePath()
    }

    static String cqlOLD = TickerSchema.insertSecondSchedulesAllCols('OLD', keyspace)
    static String cqlNEW = TickerSchema.insertSecondSchedulesAllCols('NEW', keyspace)

    static List data = [
            // interleaved but distinct column keys on the same rowkey
            [cqlOLD, [100000L, 'group1', 'name1', 'jobtype1', 'dataA', 'expressionA', 1L, 'statusA', 'timezoneA'] as Object[]],
            [cqlNEW, [100000L, 'group1', 'name2', 'jobtype2', 'dataB', 'expressionB', 2L, 'statusB', 'timezoneB'] as Object[]],
            [cqlOLD, [100000L, 'group1', 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlNEW, [100000L, 'group1', 'name4', 'jobtype2', 'dataB', 'expressionB', 2L, 'statusB', 'timezoneB'] as Object[]],
            [cqlOLD, [100000L, 'group1', 'name4', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlNEW, [100000L, 'group2', 'name2', 'jobtype2', 'dataB', 'expressionB', 2L, 'statusB', 'timezoneB'] as Object[]],
            [cqlOLD, [100000L, 'group2', 'name2', 'jobtype5', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],

            // row keys only in NEW
            [cqlNEW, [200001L, 'group1', 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlNEW, [200017L, 'group1', 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlNEW, [200016L, 'group1', 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlNEW, [200015L, 'group1', 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlOLD, [200015L, 'group1', 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlNEW, [200011L, 'group1', 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlNEW, [200021L, 'group1', 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlNEW, [200027L, 'group1', 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlOLD, [200026L, 'group1', 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlNEW, [200025L, 'group1', 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlNEW, [200021L, 'group1', 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],

            // row key only in OLD
            [cqlOLD, [300003L, 'group1', 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlOLD, [300035L, 'group1', 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlNEW, [300034L, 'group1', 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlOLD, [300033L, 'group1', 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlOLD, [300032L, 'group1', 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlOLD, [300031L, 'group1', 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlNEW, [300013L, 'group1', 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlOLD, [300015L, 'group1', 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlOLD, [300014L, 'group1', 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlNEW, [300013L, 'group1', 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlOLD, [300012L, 'group1', 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlOLD, [300011L, 'group1', 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],

            // row with colliding row + column key in both tables (by default, we take the newer)
            [cqlNEW, [100004L, 'groupCON', 'nameCON', 'jobtypeCON', 'dataC', null, 3L, null, null] as Object[]],
            [cqlOLD, [100004L, 'groupCON', 'nameCON', 'jobtypeCON', null, 'expressionC', null, null, 'timezoneC'] as Object[]],
    ]

    //static List oldByHash = data.findAll{it[0] == cqlOLD}.collect{[HashU.murmur3(ByteBufU.longToBB(it[1][0])),it[1]]}.sort{long a, long b -> a[0] <=> b[0]}
    //static List newByHash = data.findAll{it[0] == cqlNEW}.collect{[HashU.murmur3(ByteBufU.longToBB(it[1][0])),it[1]]}.sort{long a, long b -> a[0] <=> b[0]}


    void 'test rowkey diff'() {
        given:
        // LOAD DATA TO DB
        data.each {
            drv.execSync(it[0],it[1])
        }

        when: 'test diff script - end resultSet on both'
        RowKeyDiff diff = new RowKeyDiff(
                left: drv = new Drv(autoStart: true).nodes("127.0.0.1").port(9142).initDataSources(),
                leftKeyspace: keyspace,
                leftTable: "second_schedulesOLD",
                right: drv = new Drv(autoStart: true).nodes("127.0.0.1").port(9142).initDataSources(),
                rightKeyspace: keyspace,
                rightTable: "second_schedulesNEW",
        )
        List<DiffKey> diffReport = Lists.newArrayList(diff.iterator())

        then:
        diffReport.find{it.key[0] == 100000L} == null
        diffReport.find{it.key[0] == 100004L} == null
        diffReport.find{it.key[0] == 200015L} == null
        diffReport.find{it.key[0] == 200021L}.side == "RIGHT"
        diffReport.find{it.key[0] == 300013L}.side == "RIGHT"
        diffReport.find{it.key[0] == 300012L}.side == "LEFT"

        when: 'test diff script - end resultSet iteration with one exhausting before the other'
        // MOAR DATA
        data.each {
            it[1][0] = 3L*it[1][0]
            drv.execSync(it[0],it[1])
            it[1][0] = 4L*it[1][0]
            drv.execSync(it[0],it[1])
        }
        diff = new RowKeyDiff(
                left: drv = new Drv(autoStart: true).nodes("127.0.0.1").port(9142).initDataSources(),
                leftKeyspace: keyspace,
                leftTable: "second_schedulesOLD",
                right: drv = new Drv(autoStart: true).nodes("127.0.0.1").port(9142).initDataSources(),
                rightKeyspace: keyspace,
                rightTable: "second_schedulesNEW",
        )
        diffReport = Lists.newArrayList(diff.iterator())

        then:
        diffReport.find{it.key[0] == 300000L} == null
        diffReport.find{it.key[0] == 400000L} == null
        diffReport.find{it.key[0] == 400016L} == null
        diffReport.find{it.key[0] == 300045L} == null
        diffReport.find{it.key[0] == 600063L}.side == "RIGHT"
        diffReport.find{it.key[0] == 4L*3L*300013L}.side == "RIGHT"
        diffReport.find{it.key[0] == 3L*300012L}.side == "LEFT"


    }

    def cleanupSpec() {
        try {
            drv.destroy()
        } catch (Exception e) {
            println "cleanup error: $e.message"
        }
    }
}
