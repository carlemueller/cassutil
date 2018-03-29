package cass.rs

import cass.drv.Drv
import cass.drv.RsIterator
import cass.drv.St
import cass.util.RowU
import com.datastax.driver.core.Row
import com.fasterxml.jackson.core.type.TypeReference
import helpers.LogUtil
import jsonutil.JSONUtil
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll
import helpers.TestSchema

class TimeBucketResultSetSpec extends Specification {

    @Shared
    static Drv drv

    static String keyspace = 'testschema'

    def setupSpec() {
        LogUtil.setLogLevel('org.apache', "ERROR")
        LogUtil.setLogLevel('io.netty', "ERROR")
        LogUtil.setLogLevel('com.datastax', "ERROR")
        LogUtil.setLogLevel('cass.drv', "ERROR")
        EmbeddedCassandraServerHelper.startEmbeddedCassandra()
        drv = new Drv(autoStart: true).nodes("127.0.0.1").port(9142)
        try {
            drv.execSync(TestSchema.CREATE_KS(1, keyspace), null)
        } catch (Exception e) {
        }
        try {
            10.times {
                drv.execSync(TestSchema.CREATE_scheduled('_st_' + it, keyspace), null)
            }
        } catch (Exception e) {
        }
        try {
            10.times {
                drv.execSync(TestSchema.CREATE_scheduled('_lt_' + it, keyspace), null)
            }
        } catch (Exception e) {
        }

        println 'testdir: ' + new File('./newfile').getAbsolutePath()
    }


    String cqlSt0 = TestSchema.insertScheduledAllCols('_st_0', keyspace)
    String cqlSt1 = TestSchema.insertScheduledAllCols('_st_1', keyspace)
    String cqlSt2 = TestSchema.insertScheduledAllCols('_st_2', keyspace)
    String cqlSt3 = TestSchema.insertScheduledAllCols('_st_3', keyspace)
    String cqlSt4 = TestSchema.insertScheduledAllCols('_st_4', keyspace)
    String cqlSt5 = TestSchema.insertScheduledAllCols('_st_5', keyspace)
    String cqlSt6 = TestSchema.insertScheduledAllCols('_st_6', keyspace)
    String cqlSt7 = TestSchema.insertScheduledAllCols('_st_7', keyspace)
    String cqlSt8 = TestSchema.insertScheduledAllCols('_st_8', keyspace)
    String cqlSt9 = TestSchema.insertScheduledAllCols('_st_9', keyspace)

    String cqlLt0 = TestSchema.insertScheduledAllCols('_lt_0', keyspace)
    String cqlLt1 = TestSchema.insertScheduledAllCols('_lt_1', keyspace)
    String cqlLt2 = TestSchema.insertScheduledAllCols('_lt_2', keyspace)
    String cqlLt3 = TestSchema.insertScheduledAllCols('_lt_3', keyspace)
    String cqlLt4 = TestSchema.insertScheduledAllCols('_lt_4', keyspace)
    String cqlLt5 = TestSchema.insertScheduledAllCols('_lt_5', keyspace)
    String cqlLt6 = TestSchema.insertScheduledAllCols('_lt_6', keyspace)
    String cqlLt7 = TestSchema.insertScheduledAllCols('_lt_7', keyspace)
    String cqlLt8 = TestSchema.insertScheduledAllCols('_lt_8', keyspace)
    String cqlLt9 = TestSchema.insertScheduledAllCols('_lt_9', keyspace)

    List dataSh1 = [
            // short term buckets
            [cqlSt0, ['group1', 10_000_010L, 'name1', 'jobtype1', 'dataA', 'expressionA', 1L, 'statusA', 'timezoneA'] as Object[]],
            [cqlSt1, ['group1', 10_100_010L, 'name1', 'jobtype1', 'dataA', 'expressionA', 1L, 'statusA', 'timezoneA'] as Object[]],
            [cqlSt2, ['group1', 10_200_000L, 'name2', 'jobtype2', 'dataB', 'expressionB', 2L, 'statusB', 'timezoneB'] as Object[]],
            [cqlSt2, ['group1', 10_200_005L, 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlSt4, ['group1', 10_400_005L, 'name4', 'jobtype2', 'dataB', 'expressionB', 2L, 'statusB', 'timezoneB'] as Object[]],
            [cqlSt7, ['group1', 10_701_000L, 'name4', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlSt7, ['groupTOO', 10_702_000L, 'name2', 'jobtype2', 'dataB', 'expressionB', 2L, 'statusB', 'timezoneB'] as Object[]],
            [cqlSt7, ['group1', 10_702_000L, 'name2', 'jobtype2', 'dataB', 'expressionB', 2L, 'statusB', 'timezoneB'] as Object[]],
            [cqlSt7, ['group1', 10_899_999L, 'name2', 'jobtype5', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
    ]

    List dataSh2 = [
            // short term bucket with failover to long term
            [cqlSt8, ['group1', 10_900_001L, 'name2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlSt8, ['group1', 10_900_017L, 'name3', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlSt8, ['group1', 10_900_016L, 'name4', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlSt8, ['group1', 10_900_015L, 'name5', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlSt8, ['group1', 10_900_011L, 'name6', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlSt8, ['group1', 10_900_021L, 'name7', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlSt8, ['group1', 10_900_021L, 'nameq', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlSt8, ['group1', 10_900_025L, 'namep', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlSt8, ['group1', 10_900_027L, 'name8', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlSt8, ['group1', 10_900_026L, 'name9', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlSt8, ['group1', 10_900_026L, 'namea', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlSt8, ['group1', 10_900_026L, 'nameb', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlSt8, ['group1', 10_900_026L, 'namec', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlSt8, ['group1', 10_900_026L, 'named', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlSt8, ['group1', 10_900_026L, 'namee', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlSt8, ['group1', 10_900_026L, 'namef', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlSt8, ['group1', 10_900_026L, 'nameg', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlSt8, ['group1', 10_900_026L, 'nameh', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlSt8, ['group1', 10_900_026L, 'namei', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlSt8, ['group1', 10_900_026L, 'namej', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlSt8, ['group1', 10_900_026L, 'namek', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlSt8, ['group1', 10_900_026L, 'namel', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlSt8, ['group1', 10_900_026L, 'namem', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlSt8, ['group1', 10_900_026L, 'namen', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlSt8, ['group1', 10_900_026L, 'nameo', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
    ]

    List dataLg = [
            [cqlLt0, ['group1', 5_300_003L, 'nameA2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlLt0, ['group1', 6_300_035L, 'nameB2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlLt1, ['group1', 15_300_034L, 'nameC2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlLt2, ['group1', 20_300_033L, 'nameD2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlLt2, ['group1', 30_300_032L, 'nameE', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlLt3, ['group1', 40_300_031L, 'nameF2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlLt5, ['group1', 50_300_013L, 'nameG2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlLt5, ['group1', 60_300_015L, 'nameH2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlLt6, ['group1', 70_300_014L, 'nameI2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlLt7, ['group1', 80_300_013L, 'nameJ2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlLt8, ['group1', 90_300_012L, 'nameK2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
            [cqlLt9, ['group1', 100_300_011L, 'nameL2', 'jobtype3', 'dataC', 'expressionC', 3L, 'statusC', 'timezoneC'] as Object[]],
    ]

    List data = dataSh1 + dataSh2 + dataLg

    @Unroll
    void 'test query of rows #desc'() {
        given:
        TimeBucketedRS rs = new TimeBucketedRS(
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
                drv: drv
        )

        //LogUtil.setLogLevel('cass.resultSet', "TRACE")

        // LOAD DATA TO DB
        data.each {
            drv.execSync(it[0], it[1])
        }

        // now filter the rows that are superseded and then we should have the expected data

        when: 'we query a second very close to the current time in the middle of the current short term bucket'
        rs.currentMillis = currentMillis
        rs.startSecond = startSecond
        List<St> queries = rs.bucketQueries(rs.startSecond, rs.currentMillis)
        rs.init()
        RsIterator<Row> iterator = new RsIterator<>(rs: rs)
        List results = iterator.toList().collect { RowU.toList(it) }

        println JSONUtil.toJSONPrettyList(results)
        if (queries) {
            queries.each {
                print it.cql
                print " -- " + it.args[1]
                print " -- " + it.args[1].intdiv(rs.millisPerShort)
                println " -- " + it.args[1].intdiv(rs.millisPerLong)
            }
        }

        then:
        results == expectedData

        where:
        desc         | currentMillis | startSecond | expectedData
        '0th bucket' | 10_000_000L   | 10_001L     | JSONUtil.deserialize(ExpectedDataOne.json, listOfLists())
        '3rd bucket' | 10_000_000L   | 10_301L     | JSONUtil.deserialize(ExpectedDataTwo.json, listOfLists())
        '7th bucket' | 10_700_000L   | 10_001L     | JSONUtil.deserialize(ExpectedDataThre.json, listOfLists())

    }

    def cleanupSpec() {
        try {
            drv.destroy()
        } catch (Exception e) {
            println "cleanup error: $e.message"
        }
    }

    static TypeReference<List<List>> listOfLists() { new TypeReference<List<List>>() {} }
}

class ExpectedDataOne {
    static String json = """[
["group1",10100010,"name1","jobtype1","dataA","expressionA",1,"statusA","timezoneA"],
["group1",10200000,"name2","jobtype2","dataB","expressionB",2,"statusB","timezoneB"],
["group1",10200005,"name2","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10400005,"name4","jobtype2","dataB","expressionB",2,"statusB","timezoneB"],
["group1",15300034,"nameC2","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",20300033,"nameD2","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",30300032,"nameE","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",40300031,"nameF2","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",50300013,"nameG2","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",60300015,"nameH2","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",70300014,"nameI2","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",80300013,"nameJ2","jobtype3","dataC","expressionC",3,"statusC","timezoneC"]
]"""
}

class ExpectedDataTwo {
    static String json = """[
["group1",10400005,"name4","jobtype2","dataB","expressionB",2,"statusB","timezoneB"],
["group1",15300034,"nameC2","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",20300033,"nameD2","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",30300032,"nameE","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",40300031,"nameF2","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",50300013,"nameG2","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",60300015,"nameH2","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",70300014,"nameI2","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",80300013,"nameJ2","jobtype3","dataC","expressionC",3,"statusC","timezoneC"]
]"""
}

// this dataset will have a recursive skip in the fetchThreshold() and starts out empty.
class ExpectedDataThre {
    static String json = """[
["group1",10701000,"name4","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10702000,"name2","jobtype2","dataB","expressionB",2,"statusB","timezoneB"],
["group1",10899999,"name2","jobtype5","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10900001,"name2","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10900011,"name6","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10900015,"name5","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10900016,"name4","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10900017,"name3","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10900021,"name7","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10900021,"nameq","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10900025,"namep","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10900026,"name9","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10900026,"namea","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10900026,"nameb","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10900026,"namec","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10900026,"named","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10900026,"namee","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10900026,"namef","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10900026,"nameg","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10900026,"nameh","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10900026,"namei","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10900026,"namej","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10900026,"namek","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10900026,"namel","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10900026,"namem","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10900026,"namen","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10900026,"nameo","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",10900027,"name8","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",15300034,"nameC2","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",20300033,"nameD2","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",30300032,"nameE","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",40300031,"nameF2","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",50300013,"nameG2","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",60300015,"nameH2","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",70300014,"nameI2","jobtype3","dataC","expressionC",3,"statusC","timezoneC"],
["group1",80300013,"nameJ2","jobtype3","dataC","expressionC",3,"statusC","timezoneC"]
]"""
}