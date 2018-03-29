package cass.drv

import com.datastax.driver.core.ColumnDefinitions
import groovy.transform.CompileStatic

@CompileStatic
trait Rs<T> {
    abstract boolean ready()
    abstract T one()
    abstract boolean isExhausted()
    abstract ColumnDefinitions getColumnDefinitions()
    Iterator<T> iterator() { new RsIterator<T>(rs: this) }
}