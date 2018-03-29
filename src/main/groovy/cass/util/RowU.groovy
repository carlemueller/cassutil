package cass.util

import com.datastax.driver.core.ColumnMetadata
import com.datastax.driver.core.Row
import groovy.transform.CompileStatic

@CompileStatic
class RowU {

    static Object[] toArray(Row row) {
        if (row == null) return null
        Object[] array = new Object[row.columnDefinitions.size()]
        for (int i = 0; i < row.columnDefinitions.size(); i++) {
            array[i] = row.getObject(i)
        }
        return array
    }

    static Object[] toArrayMD(Row row, List<ColumnMetadata> colMetadata) {
        if (row == null) return null
        Object[] rowKey = new Object[colMetadata.size()]
        for (int i = 0; i < colMetadata.size(); i++) {
            rowKey[i] = row.getObject(colMetadata[i].name)
        }
        return rowKey
    }

    static Object[] toArray(Row row, List<String> columnSubset) {
        if (row == null) return null
        Object[] rowKey = new Object[columnSubset.size()]
        for (int i = 0; i < columnSubset.size(); i++) {
            rowKey[i] = row.getObject(columnSubset[i])
        }
        return rowKey
    }

    static List toList(Row row) {
        if (row == null) return null
        List list = new ArrayList(row.columnDefinitions.size())
        for (int i = 0; i < row.columnDefinitions.size(); i++) {
            list[i] = row.getObject(i)
        }
        return list
    }

    static List toListMD(Row row, List<ColumnMetadata> colMetadata) {
        if (row == null) return null
        List list = new ArrayList(colMetadata.size())
        for (int i = 0; i < colMetadata.size(); i++) {
            list.add(row.getObject(colMetadata[i].name))
        }
        return list
    }

    static List toList(Row row, List<String> columnSubset) {
        if (row == null) return null
        List list = new ArrayList(columnSubset.size())
        for (int i = 0; i < columnSubset.size(); i++) {
            list.add(row.getObject(columnSubset[i]))
        }
        return list
    }

    static int compareRowsAsObjectArrays(Object[] left, Object[] right) {
        if (left?.length != right?.length) {
            throw new IllegalArgumentException("Compare of two row object arrays of different size")
        }
        for (int i = 0; i < left.length; i++) {
            if (left[i] == null && right[i] == null) {
                return 0
            }
            if (left[i] != null && right[i] == null) {
                return 1
            }
            if (left[i] == null && right[i] != null) {
                return -1
            }
            if (left[i]?.class != right[i]?.class) {
                throw new IllegalArgumentException("Compare of two row object arrays with type mismatch in column $i")
            }
            switch (left[i].class) {
                case Integer:
                    int result = ((Integer) left[i]) <=> ((Integer) right[i]); if (result != 0) return result; break
                case Long:
                    int result = ((Integer) left[i]) <=> ((Integer) right[i]); if (result != 0) return result; break
                case Double:
                    int result = ((Integer) left[i]) <=> ((Integer) right[i]); if (result != 0) return result; break
                case Float:
                    int result = ((Integer) left[i]) <=> ((Integer) right[i]); if (result != 0) return result; break
            // TODO: more type support
                default:
                    int result = left[i].toString() <=> right[i].toString(); if (result != 0) return result; break
            }
        }
        return 0
    }

}
