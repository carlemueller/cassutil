package cass.rs

import cass.util.RowU
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row

class RowArrayIterator implements Iterator<Object[]> {
    ResultSet resultSet
    Integer fetchThreshold

    @Override
    boolean hasNext() {
        !resultSet.exhausted
    }

    @Override
    Object[] next() {
        Row row = resultSet.one()
        if (fetchThreshold) {
            if (resultSet.getAvailableWithoutFetching() <= fetchThreshold && !resultSet.fullyFetched) {
                resultSet.fetchMoreResults()
            }
        }
        RowU.toArray(row)
    }
}