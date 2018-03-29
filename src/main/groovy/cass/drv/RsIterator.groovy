package cass.drv

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

@Slf4j
@CompileStatic
class RsIterator<T> implements Iterator<T> {
    Rs<T> rs
    RsIterator<T> rs(Rs rs) { this.rs = rs; return this }
    boolean hasNext()  { rs.ready() && !rs.exhausted }
    T next() { rs.one() }
}