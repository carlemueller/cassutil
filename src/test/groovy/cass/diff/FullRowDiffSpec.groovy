package cass.diff

import cass.drv.Drv
import com.google.common.collect.Lists
import helpers.TestSchema
import helpers.LogUtil
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import spock.lang.Shared
import spock.lang.Specification

class FullRowDiffSpec extends Specification {

    @Shared
    static Drv drv

    static String keyspace = 'testschema'

    def setupSpec() {
        LogUtil.setLogLevel('org.apache', "ERROR")
        LogUtil.setLogLevel('io.netty', "ERROR")
        LogUtil.setLogLevel('com.datastax', "ERROR")
        EmbeddedCassandraServerHelper.startEmbeddedCassandra()

        drv = new Drv(autoStart: true).nodes("127.0.0.1").port(9142)
        try {
            drv.execSync(TestSchema.CREATE_KS(1, keyspace), null)
        } catch (Exception e) {
        }
        try {
            drv.execSync(TestSchema.CREATE_second_schedules('OLD', keyspace), null)
        } catch (Exception e) {
        }
        try {
            drv.execSync(TestSchema.CREATE_second_schedules('NEW', keyspace), null)
        } catch (Exception e) {
        }

        println 'testdir: ' + new File('./newfile').getAbsolutePath()
    }

    static String cqlOLD = TestSchema.insertSecondSchedulesAllCols('OLD', keyspace)
    static String cqlNEW = TestSchema.insertSecondSchedulesAllCols('NEW', keyspace)

    static List data = [
            // match entire row
            [cqlNEW, [100000L, 'group1', 'name4', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlOLD, [100000L, 'group1', 'name4', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],

            // last cell is different
            [cqlNEW, [100010L, 'group1', 'name4', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlOLD, [100010L, 'group1', 'name4', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'blah'] as Object[]],

            // missing a column
            [cqlNEW, [200001L, 'group1', 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlOLD, [200001L, 'group1', 'name2', 'jobtype3', 'dataC', null,          3L, 'statusC', 'timezoneC'] as Object[]],

            // interleaved but distinct column keys on the same rowkey
            [cqlOLD, [100000L, 'group1', 'name1', 'jobtype1', 'dataA', 'expressionA', 1L, 'statusA', 'timezoneA'] as Object[]],
            [cqlNEW, [100000L, 'group1', 'name2', 'jobtype2', 'dataB', 'expressionB', 2L, 'statusB', 'timezoneB'] as Object[]],
            [cqlOLD, [100000L, 'group1', 'name2', 'jobtype2', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlNEW, [100000L, 'group2', 'name2', 'jobtype2', 'dataB', 'expressionB', 2L, 'statusB', 'timezoneB'] as Object[]],
            [cqlOLD, [100000L, 'group2', 'name2', 'jobtype5', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],

            // row keys only in NEW
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


    void 'test pk diff'() {
        given:
        // LOAD DATA TO DB
        data.each {
            drv.execSync(it[0],it[1])
        }

        when: 'test diff script - end resultSet on both'
        FullRowDiff diff = new FullRowDiff(
                left: drv = new Drv(autoStart: true).nodes("127.0.0.1").port(9142).initDataSources(),
                leftKeyspace: keyspace,
                leftTable: "second_schedulesOLD",
                right: drv = new Drv(autoStart: true).nodes("127.0.0.1").port(9142).initDataSources(),
                rightKeyspace: keyspace,
                rightTable: "second_schedulesNEW",
        )
        List<DiffKey> diffReport = Lists.newArrayList(diff.iterator())

        then:
        diffReport.find{it.key == [100000L, 'group1', 'name4', 'jobtype3'] as Object[]} == null
        diffReport.find{it.key == [100010L, 'group1', 'name4', 'jobtype3'] as Object[]}.side == "DATA"
        diffReport.find{it.key == [200001L, 'group1', 'name2', 'jobtype3'] as Object[]}.side == "DATA"

    }

    def cleanupSpec() {
        try {
            drv.destroy()
        } catch (Exception e) {
            println "cleanup error: $e.message"
        }
    }
}
