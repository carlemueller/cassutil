package cass.rs

import cass.drv.Drv
import cass.drv.St
import cass.util.RowU
import com.datastax.driver.core.ResultSetFuture

class AsyncQuerySetIterator implements Iterator<Object[]> {

    AsyncQuerySetIterator(Drv drv, St st, Iterator<Object[]> cqlArgs) {
        this.st = st
        this.cqlArgs = cqlArgs
        if (!st.fetchSize) {
            st.fetchSize = 40
        }
        currentQuerySet = new ResultSetFuture[st.fetchSize]
        nextQuerySet = new ResultSetFuture[st.fetchSize]
        initiateQuerySet(currentQuerySet)
        nextQuerySetRetrieved = false
    }

    boolean hasNext() {
        for (; currentQuerySetIndex < currentQuerySet.length; currentQuerySetIndex++) {
            if (currentQuerySet[currentQuerySetIndex] != null) {
                return true
            }
        }
        // current query set is exhausted
        if (nextQuerySetRetrieved) {
            ResultSetFuture[] temp = currentQuerySet
            currentQuerySet = nextQuerySet
            nextQuerySet = temp
            nextQuerySetRetrieved = false
            currentQuerySetIndex = 0
            return true
        }
        // completely exhausted, apparently
        return false
    }

    Object[] next() {
        for (; currentQuerySetIndex < currentQuerySet.length; currentQuerySetIndex++) {
            if (currentQuerySet[currentQuerySetIndex] != null) {
                if (!nextQuerySetRetrieved && currentQuerySetIndex > st.fetchSize.intdiv(2)) {
                    initiateQuerySet(nextQuerySet)
                }
                ResultSetFuture rsf = currentQuerySet[currentQuerySetIndex]
                currentQuerySet[currentQuerySetIndex] = null
                return RowU.toArray(rsf.get().one())
            }
        }
        // hasNext() should prevent this from happening.
    }

    private St st
    private Iterator<Object[]> cqlArgs
    private ResultSetFuture[] currentQuerySet
    private ResultSetFuture[] nextQuerySet
    private boolean nextQuerySetRetrieved
    private int currentQuerySetIndex
    private Drv drv

    private void initiateQuerySet(ResultSetFuture[] querySet) {
        int i = 0
        while (i < st.fetchSize && cqlArgs.hasNext()) {
            st.args = cqlArgs.next()
            querySet[i] = drv.execAsync(st)
            i++
        }
        if (i == 0) {
            nextQuerySetRetrieved = false
        } else {
            nextQuerySetRetrieved = true
        }
    }

}