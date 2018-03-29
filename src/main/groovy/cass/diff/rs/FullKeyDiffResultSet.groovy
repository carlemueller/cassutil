package cass.diff.rs

import cass.diff.DiffKey
import cass.drv.Rs
import cass.util.RowU
import com.datastax.driver.core.ColumnDefinitions
import com.datastax.driver.core.ColumnMetadata
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row

/**
 * The queries that produced the resultsets (leftRS, rightRS) must have the token value as the 0th column
 *
 */

class FullKeyDiffResultSet implements Rs<DiffKey> {
    List<ColumnMetadata> leftRKMeta
    List<ColumnMetadata> rightRKMeta
    List<ColumnMetadata> leftCKMeta
    List<ColumnMetadata> rightCKMeta
    ResultSet leftRS
    ResultSet rightRS

    int leftThreshold = 1000
    int rightThreshold = 1000

    private Row currentleftRSRow
    private Row currentrightRSRow
    private String lastRowSource = 'init'

    Comparator<Row> rowComparator = new Comparator<Row>() {
        int compare(Row o1, Row o2) {
            o1.partitionKeyToken <=> o2.partitionKeyToken ?:
                    RowU.compareRowsAsObjectArrays(RowU.toArrayMD(o1,leftRKMeta),RowU.toArrayMD(o2,rightRKMeta)) ?:
                            RowU.compareRowsAsObjectArrays(RowU.toArrayMD(o1,leftCKMeta),RowU.toArrayMD(o2,rightCKMeta))
        }
    }

    ColumnDefinitions getColumnDefinitions() { leftRS.columnDefinitions }

    DiffKey one() {
        while (true) {
            if (lastRowSource == 'init') {
                currentleftRSRow = leftRS.one()
                currentrightRSRow = rightRS.one()
            } else if (lastRowSource == 'old') {
                currentleftRSRow = leftRS.one()
            } else if (lastRowSource == 'new') {
                currentrightRSRow = rightRS.one()
            } else if (lastRowSource == 'both') {
                currentleftRSRow = leftRS.one()
                currentrightRSRow = rightRS.one()
            }
            fetchMore()
            if (currentrightRSRow == null && currentleftRSRow == null) {
                return null
            }
            int compareResult = currentrightRSRow == null && currentleftRSRow != null ? -1 :
                    currentrightRSRow != null && currentleftRSRow == null ? 1 :
                            rowComparator.compare(currentleftRSRow, currentrightRSRow)
            if (compareResult == 0) {
                lastRowSource = 'both'
                currentrightRSRow = null
                currentleftRSRow = null
                // no diff, loop until ResultSets are exhausted or a diff is encountered.
            } else if (compareResult < 0) {
                lastRowSource = 'old'
                Row rowToReturn = currentleftRSRow
                currentleftRSRow = null
                return new DiffKey(token: rowToReturn.getLong(0), key: RowU.toArrayMD(rowToReturn, leftRKMeta+leftCKMeta), side: 'LEFT')
            } else {
                lastRowSource = 'new'
                Row rowToReturn = currentrightRSRow
                currentrightRSRow = null
                return new DiffKey(token: rowToReturn.getLong(0), key: RowU.toArrayMD(rowToReturn, rightRKMeta+rightCKMeta), side: 'RIGHT')
            }
        }
    }

    boolean fetchMore() {
        boolean fetchTriggered = false
        if (leftRS.getAvailableWithoutFetching() <= leftThreshold && !leftRS.isFullyFetched()) {
            leftRS.fetchMoreResults()
            fetchTriggered = true
        }
        if (rightRS.getAvailableWithoutFetching() <= rightThreshold && !rightRS.isFullyFetched()) {
            rightRS.fetchMoreResults()
            fetchTriggered = true
        }
        return fetchTriggered
    }

    boolean ready() {
        rightRS != null && leftRS != null
    }

    boolean isExhausted() {
        if (leftRS.exhausted && rightRS.exhausted) {
            if (lastRowSource == 'init') { return true }
            if (currentleftRSRow == null && currentrightRSRow == null) { return true }
            else {return false}
        }
        return false
    }

}