package cass.rs

import com.datastax.driver.core.*
import com.google.common.reflect.TypeToken
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.ByteBuffer

class TimeBucketingCollatingSpec extends Specification {

    @Unroll
    void 'Test comparator #reason'() {
        given:

        TimeBucketedCollatingRS rs = new TimeBucketedCollatingRS()

        when: 'we compare rows '
        Row r1 = new RowFacade(data: row1)
        Row r2 = new RowFacade(data: row2)
        int cmp = rs.compare(r1, r2)

        then: 'get expected picked row'
        exp == 0 ? cmp == 0 : exp < 0 ? cmp < 0 : cmp > 0

        where:
        row1                                                 | row2                                                  | exp | reason
        ["g1", 409968141538L, "nm3", "jobtype1", "longdata"] | ["g1", 409968141538L, "nm8", "jobtype1", "shortdata"] | -1  | 'diff names'
        ["g1", 409968141538L, "nm3", "jobtype1", "longdata"] | ["g1", 409968141538L, "nm3", "jobtype1", "shortdata"] | 0   | 'same keys'
        ["g1", 409968141538L, "nm8", "jobtype1", "longdata"] | ["g1", 409968141538L, "nm3", "jobtype1", "shortdata"] | 1   | 'diff names (invert)'
        ["g1", 409968141534L, "nm3", "jobtype1", "longdata"] | ["g1", 409968141538L, "nm3", "jobtype1", "shortdata"] | -1  | 'diff second'
        ["g1", 409968141538L, "nm3", "jobtype1", "longdata"] | ["g1", 409968141534L, "nm3", "jobtype1", "shortdata"] | 1   | 'diff second (invert)'
    }


    @Unroll
    void 'Test minIdx #reason'() {
        given:

        TimeBucketedCollatingRS rs = new TimeBucketedCollatingRS()

        when: 'we compare rows '

        int idx = rs.min([new RowFacade(data: data[0]), new RowFacade(data: data[1]), data[2] == null ? null : new RowFacade(data: data[2])] as Row[])

        then: 'get expected picked row'
        exp == idx

        where:
        data                                                                                                            | exp | reason
        [["g1", 409968141538L, "nm8", "jt1"], ["g1", 409968141538L, "nm3", "jt1"], ["g1", 409968221538L, "nm3", "jt1"]] | 1   | 'short'
        data1()                                                                                                         | 1   | 'replicate failure'
        data1NoLeg()                                                                                                    | 1   | 'replicate failure'
        data1NullLeg()                                                                                                  | 1   | 'replicate failure'

    }

    static List data1() {
        [
                ['group1', 409968141538L, 'name8', 'jobtype1', 'shortdata', 'expr', 1, 'good', 'UTC'],
                ['group1', 409968141538L, 'name3', 'jobtype1', 'longdata', 'expr', 1, 'good', 'UTC'],
                ['group1', 409968212460L, 'name5', 'jobtype2', 'legacy', 'expr', 1, 'good', 'UTC'],
        ]
    }

    static List data1NoLeg() {
        [
                ['group1', 409968141538L, 'name8', 'jobtype1', 'shortdata', 'expr', 1, 'good', 'UTC'],
                ['group1', 409968141538L, 'name3', 'jobtype1', 'longdata', 'expr', 1, 'good', 'UTC'],
        ]
    }

    static List data1NullLeg() {
        [
                ['group1', 409968141538L, 'name8', 'jobtype1', 'shortdata', 'expr', 1, 'good', 'UTC'],
                ['group1', 409968141538L, 'name3', 'jobtype1', 'longdata', 'expr', 1, 'good', 'UTC'],
                null
        ]
    }

}

class RowFacade implements Row {
    List data

    @Override
    ColumnDefinitions getColumnDefinitions() {
        return null
    }

    @Override
    Token getToken(int i) {
        return null
    }

    @Override
    Token getToken(String name) {
        return null
    }

    @Override
    Token getPartitionKeyToken() {
        return null
    }

    @Override
    boolean isNull(int i) {
        return data[i] == null
    }

    @Override
    boolean getBool(int i) {
        return false
    }

    @Override
    byte getByte(int i) {
        return data[i]
    }

    @Override
    short getShort(int i) {
        return data[i]
    }

    @Override
    int getInt(int i) {
        return data[i]
    }

    @Override
    long getLong(int i) {
        return data[i]
    }

    @Override
    Date getTimestamp(int i) {
        return data[i]
    }

    @Override
    LocalDate getDate(int i) {
        return data[i]
    }

    @Override
    long getTime(int i) {
        return data[i]
    }

    @Override
    float getFloat(int i) {
        return data[i]
    }

    @Override
    double getDouble(int i) {
        return data[i]
    }

    @Override
    ByteBuffer getBytesUnsafe(int i) {
        return null
    }

    @Override
    ByteBuffer getBytes(int i) {
        return null
    }

    @Override
    String getString(int i) {
        return data[i]
    }

    @Override
    BigInteger getVarint(int i) {
        return data[i]
    }

    @Override
    BigDecimal getDecimal(int i) {
        return data[i]
    }

    @Override
    UUID getUUID(int i) {
        return data[i]
    }

    @Override
    InetAddress getInet(int i) {
        return data[i]
    }

    @Override
    <T> List<T> getList(int i, Class<T> elementsClass) {
        return null
    }

    @Override
    <T> List<T> getList(int i, TypeToken<T> elementsType) {
        return null
    }

    @Override
    <T> Set<T> getSet(int i, Class<T> elementsClass) {
        return null
    }

    @Override
    <T> Set<T> getSet(int i, TypeToken<T> elementsType) {
        return null
    }

    @Override
    <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass) {
        return null
    }

    @Override
    <K, V> Map<K, V> getMap(int i, TypeToken<K> keysType, TypeToken<V> valuesType) {
        return null
    }

    @Override
    UDTValue getUDTValue(int i) {
        return null
    }

    @Override
    TupleValue getTupleValue(int i) {
        return null
    }

    @Override
    Object getObject(int i) {
        return data[i]
    }

    @Override
    <T> T get(int i, Class<T> targetClass) {
        return null
    }

    @Override
    <T> T get(int i, TypeToken<T> targetType) {
        return null
    }

    @Override
    <T> T get(int i, TypeCodec<T> codec) {
        return null
    }

    @Override
    boolean isNull(String name) {
        return false
    }

    @Override
    boolean getBool(String name) {
        return false
    }

    @Override
    byte getByte(String name) {
        return 0
    }

    @Override
    short getShort(String name) {
        return 0
    }

    @Override
    int getInt(String name) {
        return 0
    }

    @Override
    long getLong(String name) {
        return 0
    }

    @Override
    Date getTimestamp(String name) {
        return null
    }

    @Override
    LocalDate getDate(String name) {
        return null
    }

    @Override
    long getTime(String name) {
        return 0
    }

    @Override
    float getFloat(String name) {
        return 0
    }

    @Override
    double getDouble(String name) {
        return 0
    }

    @Override
    ByteBuffer getBytesUnsafe(String name) {
        return null
    }

    @Override
    ByteBuffer getBytes(String name) {
        return null
    }

    @Override
    String getString(String name) {
        return null
    }

    @Override
    BigInteger getVarint(String name) {
        return null
    }

    @Override
    BigDecimal getDecimal(String name) {
        return null
    }

    @Override
    UUID getUUID(String name) {
        return null
    }

    @Override
    InetAddress getInet(String name) {
        return null
    }

    @Override
    def <T> List<T> getList(String name, Class<T> elementsClass) {
        return null
    }

    @Override
    def <T> List<T> getList(String name, TypeToken<T> elementsType) {
        return null
    }

    @Override
    def <T> Set<T> getSet(String name, Class<T> elementsClass) {
        return null
    }

    @Override
    def <T> Set<T> getSet(String name, TypeToken<T> elementsType) {
        return null
    }

    @Override
    def <K, V> Map<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
        return null
    }

    @Override
    def <K, V> Map<K, V> getMap(String name, TypeToken<K> keysType, TypeToken<V> valuesType) {
        return null
    }

    @Override
    UDTValue getUDTValue(String name) {
        return null
    }

    @Override
    TupleValue getTupleValue(String name) {
        return null
    }

    @Override
    Object getObject(String name) {
        return null
    }

    @Override
    def <T> T get(String name, Class<T> targetClass) {
        return null
    }

    @Override
    def <T> T get(String name, TypeToken<T> targetType) {
        return null
    }

    @Override
    def <T> T get(String name, TypeCodec<T> codec) {
        return null
    }
}
