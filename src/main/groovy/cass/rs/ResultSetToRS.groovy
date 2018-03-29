package cass.rs

import cass.drv.Rs
import com.datastax.driver.core.ColumnDefinitions
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row

/**
 * simple wrapper of a result Set, optionally with minimally complicated fetching threshold logic
 *
 */
class ResultSetToRS implements Rs<Row> {

    ResultSetToRS resultSet(ResultSet rs) { this.resultSet = rs; return this}
    ResultSetToRS fetchThreshold(int fetchMore) { this.fetchThreshold = fetchMore; return this}

    ResultSet resultSet
    Integer fetchThreshold

    @Override
    boolean ready() { resultSet != null }

    @Override
    ColumnDefinitions getColumnDefinitions() { resultSet.columnDefinitions }

    @Override
    Row one() {
        fetchMore()
        return resultSet.one()
    }

    boolean fetchMore() {
        if (fetchThreshold != null) {
            if (!resultSet.exhausted && !resultSet.fullyFetched && resultSet.availableWithoutFetching <= fetchThreshold) {
                resultSet.fetchMoreResults()
                return true
            }
        }
        return false
    }

    @Override
    boolean isExhausted() { resultSet.exhausted }

}
