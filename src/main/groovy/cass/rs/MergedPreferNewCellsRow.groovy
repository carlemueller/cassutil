package cass.rs

import com.datastax.driver.core.*
import com.google.common.reflect.TypeToken
import groovy.transform.CompileStatic

import java.nio.ByteBuffer

@CompileStatic
class MergedPreferNewCellsRow implements Row {
    Row oldRow
    Row newRow

    @Override
    ColumnDefinitions getColumnDefinitions() { oldRow.columnDefinitions }

    @Override
    Token getToken(int i) { oldRow.getToken(i) }

    @Override
    Token getToken(String name) { oldRow.getToken(name) }

    @Override
    Token getPartitionKeyToken() { oldRow.partitionKeyToken }

    @Override
    boolean isNull(int i) { oldRow.isNull(i) && newRow.isNull(i) }

    @Override
    boolean isNull(String i) { oldRow.isNull(i) && newRow.isNull(i) }

    @Override
    boolean getBool(int i) { newRow.isNull(i) ? oldRow.getBool(i) : newRow.getBool(i) }

    @Override
    boolean getBool(String s) { newRow.isNull(s) ? oldRow.getBool(s) : newRow.getBool(s) }

    @Override
    byte getByte(int i) { newRow.isNull(i) ? oldRow.getByte(i) : newRow.getByte(i) }

    @Override
    byte getByte(String i) { newRow.isNull(i) ? oldRow.getByte(i) : newRow.getByte(i) }

    @Override
    short getShort(int i) { newRow.isNull(i) ? oldRow.getShort(i) : newRow.getShort(i) }

    @Override
    short getShort(String i) { newRow.isNull(i) ? oldRow.getShort(i) : newRow.getShort(i) }

    @Override
    int getInt(int i) { newRow.isNull(i) ? oldRow.getInt(i) : newRow.getInt(i) }

    @Override
    int getInt(String i) { newRow.isNull(i) ? oldRow.getInt(i) : newRow.getInt(i) }

    @Override
    long getLong(int i) { newRow.isNull(i) ? oldRow.getLong(i) : newRow.getLong(i) }

    @Override
    long getLong(String i) { newRow.isNull(i) ? oldRow.getLong(i) : newRow.getLong(i) }

    @Override
    Date getTimestamp(int i) { newRow.isNull(i) ? oldRow.getTimestamp(i) : newRow.getTimestamp(i) }

    @Override
    Date getTimestamp(String i) { newRow.isNull(i) ? oldRow.getTimestamp(i) : newRow.getTimestamp(i) }

    @Override
    LocalDate getDate(int i) { newRow.isNull(i) ? oldRow.getDate(i) : newRow.getDate(i) }

    @Override
    LocalDate getDate(String i) { newRow.isNull(i) ? oldRow.getDate(i) : newRow.getDate(i) }

    @Override
    long getTime(int i) { newRow.isNull(i) ? oldRow.getTime(i) : newRow.getTime(i) }

    @Override
    long getTime(String i) { newRow.isNull(i) ? oldRow.getTime(i) : newRow.getTime(i) }

    @Override
    float getFloat(int i) { newRow.isNull(i) ? oldRow.getFloat(i) : newRow.getFloat(i) }

    @Override
    float getFloat(String i) { newRow.isNull(i) ? oldRow.getFloat(i) : newRow.getFloat(i) }

    @Override
    double getDouble(int i) { newRow.isNull(i) ? oldRow.getDouble(i) : newRow.getDouble(i) }

    @Override
    double getDouble(String i) { newRow.isNull(i) ? oldRow.getDouble(i) : newRow.getDouble(i) }

    @Override
    ByteBuffer getBytesUnsafe(int i) { newRow.isNull(i) ? oldRow.getBytesUnsafe(i) : newRow.getBytesUnsafe(i) }

    @Override
    ByteBuffer getBytesUnsafe(String i) { newRow.isNull(i) ? oldRow.getBytesUnsafe(i) : newRow.getBytesUnsafe(i) }

    @Override
    ByteBuffer getBytes(int i) { newRow.isNull(i) ? oldRow.getBytes(i) : newRow.getBytes(i) }

    @Override
    ByteBuffer getBytes(String i) { newRow.isNull(i) ? oldRow.getBytes(i) : newRow.getBytes(i) }

    @Override
    String getString(int i) { newRow.isNull(i) ? oldRow.getString(i) : newRow.getString(i) }

    @Override
    String getString(String i) { newRow.isNull(i) ? oldRow.getString(i) : newRow.getString(i) }

    @Override
    BigInteger getVarint(int i) { newRow.isNull(i) ? oldRow.getVarint(i) : newRow.getVarint(i) }

    @Override
    BigInteger getVarint(String i) { newRow.isNull(i) ? oldRow.getVarint(i) : newRow.getVarint(i) }

    @Override
    BigDecimal getDecimal(int i) { newRow.isNull(i) ? oldRow.getDecimal(i) : newRow.getDecimal(i) }

    @Override
    BigDecimal getDecimal(String i) { newRow.isNull(i) ? oldRow.getDecimal(i) : newRow.getDecimal(i) }

    @Override
    UUID getUUID(int i) { newRow.isNull(i) ? oldRow.getUUID(i) : newRow.getUUID(i) }

    @Override
    UUID getUUID(String i) { newRow.isNull(i) ? oldRow.getUUID(i) : newRow.getUUID(i) }

    @Override
    InetAddress getInet(int i) { newRow.isNull(i) ? oldRow.getInet(i) : newRow.getInet(i) }

    @Override
    InetAddress getInet(String i) { newRow.isNull(i) ? oldRow.getInet(i) : newRow.getInet(i) }

    @Override
    <T> List<T> getList(int i, Class<T> elementsClass) {
        newRow.isNull(i) ? oldRow.getList(i, elementsClass) : newRow.getList(i, elementsClass)
    }

    @Override
    <T> List<T> getList(int i, TypeToken<T> elementsClass) {
        newRow.isNull(i) ? oldRow.getList(i, elementsClass) : newRow.getList(i, elementsClass)
    }

    @Override
    <T> List<T> getList(String i, Class<T> elementsClass) {
        newRow.isNull(i) ? oldRow.getList(i, elementsClass) : newRow.getList(i, elementsClass)
    }

    @Override
    <T> List<T> getList(String i, TypeToken<T> elementsClass) {
        newRow.isNull(i) ? oldRow.getList(i, elementsClass) : newRow.getList(i, elementsClass)
    }

    @Override
    <T> Set<T> getSet(int i, Class<T> elementsClass) {
        newRow.isNull(i) ? oldRow.getSet(i, elementsClass) : newRow.getSet(i, elementsClass)
    }

    @Override
    <T> Set<T> getSet(int i, TypeToken<T> elementsClass) {
        newRow.isNull(i) ? oldRow.getSet(i, elementsClass) : newRow.getSet(i, elementsClass)
    }

    @Override
    <T> Set<T> getSet(String i, Class<T> elementsClass) {
        newRow.isNull(i) ? oldRow.getSet(i, elementsClass) : newRow.getSet(i, elementsClass)
    }

    @Override
    <T> Set<T> getSet(String i, TypeToken<T> elementsClass) {
        newRow.isNull(i) ? oldRow.getSet(i, elementsClass) : newRow.getSet(i, elementsClass)
    }

    @Override
    <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass) {
        newRow.isNull(i) ? oldRow.getMap(i, keysClass, valuesClass) : newRow.getMap(i, keysClass, valuesClass)
    }

    @Override
    <K, V> Map<K, V> getMap(int i, TypeToken<K> keysClass, TypeToken<V> valuesClass) {
        newRow.isNull(i) ? oldRow.getMap(i, keysClass, valuesClass) : newRow.getMap(i, keysClass, valuesClass)
    }

    @Override
    <K, V> Map<K, V> getMap(String i, Class<K> keysClass, Class<V> valuesClass) {
        newRow.isNull(i) ? oldRow.getMap(i, keysClass, valuesClass) : newRow.getMap(i, keysClass, valuesClass)
    }

    @Override
    <K, V> Map<K, V> getMap(String i, TypeToken<K> keysClass, TypeToken<V> valuesClass) {
        newRow.isNull(i) ? oldRow.getMap(i, keysClass, valuesClass) : newRow.getMap(i, keysClass, valuesClass)
    }

    @Override
    <T> T get(int i, Class<T> targetClass) {
        newRow.isNull(i) ? oldRow.get(i, targetClass) : newRow.get(i, targetClass)
    }

    @Override
    <T> T get(int i, TypeToken<T> targetClass) {
        newRow.isNull(i) ? oldRow.get(i, targetClass) : newRow.get(i, targetClass)
    }

    @Override
    <T> T get(int i, TypeCodec<T> targetClass) {
        newRow.isNull(i) ? oldRow.get(i, targetClass) : newRow.get(i, targetClass)
    }

    @Override
    <T> T get(String i, Class<T> targetClass) {
        newRow.isNull(i) ? oldRow.get(i, targetClass) : newRow.get(i, targetClass)
    }

    @Override
    <T> T get(String i, TypeToken<T> targetClass) {
        newRow.isNull(i) ? oldRow.get(i, targetClass) : newRow.get(i, targetClass)
    }

    @Override
    <T> T get(String i, TypeCodec<T> targetClass) {
        newRow.isNull(i) ? oldRow.get(i, targetClass) : newRow.get(i, targetClass)
    }

    @Override
    UDTValue getUDTValue(int i) { newRow.isNull(i) ? oldRow.getUDTValue(i) : newRow.getUDTValue(i) }

    @Override
    UDTValue getUDTValue(String i) { newRow.isNull(i) ? oldRow.getUDTValue(i) : newRow.getUDTValue(i) }

    @Override
    TupleValue getTupleValue(int i) { newRow.isNull(i) ? oldRow.getTupleValue(i) : newRow.getTupleValue(i) }

    @Override
    TupleValue getTupleValue(String i) { newRow.isNull(i) ? oldRow.getTupleValue(i) : newRow.getTupleValue(i) }

    @Override
    Object getObject(int i) { newRow.isNull(i) ? oldRow.getObject(i) : newRow.getObject(i) }

    @Override
    Object getObject(String i) { newRow.isNull(i) ? oldRow.getObject(i) : newRow.getObject(i) }

}