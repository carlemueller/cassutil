package cass.rs

import cass.drv.Rs
import com.datastax.driver.core.ColumnDefinitions
import com.datastax.driver.core.ExecutionInfo
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.google.common.collect.Lists
import com.google.common.util.concurrent.ListenableFuture

class RsToResultSet implements ResultSet {
    Rs<Row> rs
    RsToResultSet rs(Rs<Row> rs) { this.rs = rs; return this }

    @Override
    Row one() { rs.one() }

    @Override
    boolean isExhausted() { rs.exhausted }

    @Override
    List<Row> all() { Lists.newArrayList(iterator()) }

    @Override
    Iterator<Row> iterator() { rs.iterator() }

    @Override
    ColumnDefinitions getColumnDefinitions() {
        throw new UnsupportedOperationException("not supported, override for specific wrapped rs type")
    }

    @Override
    boolean wasApplied() {
        throw new UnsupportedOperationException("not supported, override for specific wrapped rs type")
    }

    @Override
    boolean isFullyFetched() {
        throw new UnsupportedOperationException("wrapping Rs manages fetches")
    }

    @Override
    int getAvailableWithoutFetching() {
        throw new UnsupportedOperationException("wrapping Rs manages fetches")
    }

    @Override
    ListenableFuture<ResultSet> fetchMoreResults() {
        throw new UnsupportedOperationException("wrapping Rs manages fetches")
    }

    @Override
    ExecutionInfo getExecutionInfo() {
        throw new UnsupportedOperationException("not supported, override for specific wrapped rs type")
    }

    @Override
    List<ExecutionInfo> getAllExecutionInfo() {
        throw new UnsupportedOperationException("not supported, override for specific wrapped rs type")
    }
}
