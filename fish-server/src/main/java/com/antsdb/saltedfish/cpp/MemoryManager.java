/*-------------------------------------------------------------------------------------------------
 _______ __   _ _______ _______ ______  ______
 |_____| | \  |    |    |______ |     \ |_____]
 |     | |  \_|    |    ______| |_____/ |_____]

 Copyright (c) 2016, antsdb.com and/or its affiliates. All rights reserved. *-xguo0<@

 This program is free software: you can redistribute it and/or modify it under the terms of the
 GNU GNU Lesser General Public License, version 3, as published by the Free Software Foundation.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/lgpl-3.0.en.html>
-------------------------------------------------------------------------------------------------*/
package com.antsdb.saltedfish.cpp;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.LatencyDetector;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * as subject
 *  
 * @author wgu0
 */
public final class MemoryManager {
    static Logger _log = UberUtil.getThisLogger();

    @SuppressWarnings("unchecked")
    static ConcurrentLinkedQueue<ByteBuffer>[] _array = new ConcurrentLinkedQueue[32];
    static AtomicLong _allocated = new AtomicLong();
    static AtomicLong _pooled = new AtomicLong();
    static boolean _isTraceEnabled = false;
    static ConcurrentMap<Long, StackTraceElement[]> _traces = new ConcurrentHashMap<>();
    static ThreadLocal<ConcurrentLinkedQueue<ByteBuffer>[]> _local = new ThreadLocal<>();
    static {
        for (int i=0; i<_array.length; i++) {
            _array[i] = new ConcurrentLinkedQueue<ByteBuffer>();
        }
    }
    
    /*
    private static ConcurrentLinkedQueue<ByteBuffer>[] getArray() {
        return _array;
    }
    */

    @SuppressWarnings("unchecked")
    private static ConcurrentLinkedQueue<ByteBuffer>[] getArray() {
        ConcurrentLinkedQueue<ByteBuffer>[] result = _local.get();
        if (result == null) {
            synchronized(MemoryManager.class) {
                result = _local.get();
                if (result == null) {
                    result = new ConcurrentLinkedQueue[32];
                    for (int i=0; i<result.length; i++) {
                        result[i] = new ConcurrentLinkedQueue<ByteBuffer>();
                    }
                    _local.set(result);
                }
            }
        }
        return result;
    }
    
    public static ByteBuffer alloc(int size) {
        ByteBuffer result = LatencyDetector.run(_log, "alloc0", ()->{
           return alloc0(size); 
        });
        return result;
    }
    
    private static ByteBuffer alloc0(int size) {
        int index = 32 - Integer.numberOfLeadingZeros(size-1);
        Queue<ByteBuffer> q = getArray()[index];
        ByteBuffer buf = q.poll();
        if (buf == null) {
            int roundedSize = 1 << index;
            buf = ByteBuffer.allocateDirect(roundedSize);
            buf.order(ByteOrder.nativeOrder());
            _allocated.getAndAdd(size);
        }
        else {
            _pooled.getAndAdd(-buf.capacity());
        }
        if (_isTraceEnabled) {
            _traces.put(UberUtil.getAddress(buf), Thread.currentThread().getStackTrace());
        }
        return buf;
    }
    
    public static void free(ByteBuffer buf) {
        int index = 32 - Integer.numberOfLeadingZeros(buf.capacity()-1);
        Queue<ByteBuffer> q = getArray()[index];
        buf.clear();
        q.offer(buf);
        _pooled.getAndAdd(buf.capacity());
    }

    public static void threadEnd() {
        _local.remove();
    }
    
    public static void report() {
        _log.info("allocated: {}", _allocated);
        _log.info("pooled: {}" , _pooled);
        _traces.entrySet().forEach(it -> {
            _log.info("leaked memory : {} {}", it.getKey(), toString(it.getValue()));
        });
    }
    
    private static Object toString(StackTraceElement[] value) {
        StringBuilder buf = new StringBuilder();
        buf.append('\n');
        for (int i=0; i<value.length; i++) {
            buf.append('\t');
            buf.append(value[i].toString());
            buf.append('\n');
        }
        return buf.toString();
    }

    public static long getAllocated() {
        return _allocated.get();
    }

    public static long getPooled() {
        return _pooled.get();
    }
}
