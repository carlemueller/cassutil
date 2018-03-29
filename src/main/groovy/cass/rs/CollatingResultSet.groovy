package cass.rs

import cass.drv.RsIterator
import com.datastax.driver.core.ColumnDefinitions
import com.datastax.driver.core.ExecutionInfo
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.google.common.collect.Lists
import com.google.common.util.concurrent.ListenableFuture
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * A "result set" wrapper that will merge the results of two tables, preferring the results of the "newer"
 * over the "older" table. Rows are merged at a cell level, with column values from the "newer" table row having
 * precedence over the "older" table row.
 *
 * If the "newer" row has a null column value, it falls through to the "older" row's value (which may be null too)
 *
 * A comparator that compares the row key and column key must be provided to order the rows properly
 *
 * Thus, the query must likely have the rowkey token and column key columns in it.
 *
 */

@Slf4j
@CompileStatic
class CollatingResultSet implements ResultSet {
    ResultSet oldRS
    ResultSet newRS

    Integer oldThreshold = null
    Integer newThreshold = null

    private Row currentOldRow = null
    private Row currentNewRow = null
    private String lastRowSource = 'both'

    Comparator<Row> rowComparator

    boolean mergeRows = false

    boolean initialized = false

    boolean ready() { oldRS && newRS && rowComparator }

    CollatingResultSet init() {
        assert newRS : "CollatingResultSet: primary resultset not set"
        assert oldRS : "CollatingResultSet: secondary resultset not set"
        assert rowComparator : "CollatingResultSet: row comparator not set"
        initialized = true
        return this
    }

    String getLastRowSource() { lastRowSource } // make read-only
    ColumnDefinitions getColumnDefinitions() { newRS.columnDefinitions }

    Iterator<Row> iterator() {
        if (!initialized) init()
        new RsIterator<>(rs: new ResultSetToRS(resultSet: this))
    }

    Row one() {
        if (!initialized) init()
        fetchMoreResults()
        if (lastRowSource == 'old') {
            currentOldRow = oldRS.one()
        } else if (lastRowSource == 'new') {
            currentNewRow = newRS.one()
        } else if (lastRowSource == 'both') {
            currentOldRow = oldRS.one()
            currentNewRow = newRS.one()
        }
        if (currentNewRow == null && currentOldRow == null) {
            return null
        }
        int compareResult = currentNewRow == null && currentOldRow != null ? -1 :
                currentNewRow != null && currentOldRow == null ? 1 :
                        rowComparator.compare(currentOldRow, currentNewRow)
        if (compareResult == 0) {
            lastRowSource = 'both'
            Row rowToReturn = mergeRows ? new MergedPreferNewCellsRow(oldRow: currentOldRow, newRow: currentNewRow) : currentNewRow
            currentNewRow = null
            currentOldRow = null
            return rowToReturn
        } else if (compareResult < 0) {
            lastRowSource = 'old'
            Row rowToReturn = currentOldRow
            currentOldRow = null
            return rowToReturn
        } else {
            lastRowSource = 'new'
            Row rowToReturn = currentNewRow
            currentNewRow = null
            return rowToReturn
        }
    }

    boolean isExhausted() {
        if (!initialized) init()
        if (oldRS.exhausted && newRS.exhausted) {
            if (lastRowSource == 'init') {
                return false
            }
            if (currentNewRow == null && currentOldRow == null) {
                return true
            }
        }
        return false
    }

    /**
     * fetching is internally managed in this RS, so returns null.
     *
     * @return
     */
    @Override
    ListenableFuture<ResultSet> fetchMoreResults() {
        if (!initialized) init()
        if (oldThreshold != null && !oldRS.isFullyFetched() && oldRS.getAvailableWithoutFetching() <= oldThreshold) {
            log.debug("a")
            oldRS.fetchMoreResults()
        }
        if (newThreshold != null && !newRS.isFullyFetched() && newRS.getAvailableWithoutFetching() <= newThreshold) {
            newRS.fetchMoreResults()
        }
    }

    @Override
    boolean wasApplied() { false }

    @Override
    boolean isFullyFetched() { oldRS.isFullyFetched() && newRS.isFullyFetched() }

    @Override
    List<ExecutionInfo> getAllExecutionInfo() { newRS.allExecutionInfo + oldRS.allExecutionInfo as List }

    @Override
    int getAvailableWithoutFetching() { Integer.MAX_VALUE }

    @Override
    List<Row> all() {
        if (!initialized) init()
        Lists.newArrayList(iterator())
    }

    @Override
    ExecutionInfo getExecutionInfo() { newRS.executionInfo }

}

