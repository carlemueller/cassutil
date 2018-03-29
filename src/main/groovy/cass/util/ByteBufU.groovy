package cass.util

import com.google.common.primitives.Ints
import com.google.common.primitives.Longs
import com.google.common.primitives.Shorts
import groovy.transform.CompileStatic

import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

@CompileStatic
class ByteBufU {

    static ByteBuffer strToBB(String s) {
        ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8))
    }


    static ByteBuffer strToBB(String s, Charset charset) {
        ByteBuffer.wrap(s.getBytes(charset ?: StandardCharsets.UTF_8))
    }

    static ByteBuffer longToBB(Long l) {
        ByteBuffer.wrap(Longs.toByteArray(l))
    }

    static ByteBuffer intToBB(Integer i) {
        ByteBuffer.wrap(Ints.toByteArray(i))
    }

    static ByteBuffer shortToBB(Short s) {
        ByteBuffer.wrap(Shorts.toByteArray(s))
    }

    static ByteBuffer floatToBB(Float f) {
        ByteBuffer.allocate(4).putFloat(f)
    }

    static ByteBuffer doubleToBB(Double d) {
        ByteBuffer.allocate(8).putDouble(d)
    }

    static ByteBuffer bytesToBB(byte[] ba) {
        ByteBuffer.wrap(ba)
    }


}
