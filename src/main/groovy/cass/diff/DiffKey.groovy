package cass.diff

import groovy.transform.CompileStatic

@CompileStatic
class DiffKey {
    long token
    Object[] key
    String side
}