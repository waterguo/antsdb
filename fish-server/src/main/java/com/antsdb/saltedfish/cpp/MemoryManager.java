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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
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
    
    final static int MAX_CACHE_BLOCK_SIZE = 16 * 1024 * 1024;
    final static int MAX_CACHE_SIZE_PER_THREAD = 64 * 1024 * 1024;
    
    static boolean _isTraceEnabled = false;
    static boolean _isDebugEnabled = true;
    static ThreadLocal<ThreadData> _local = ThreadLocal.withInitial(() ->{ return new ThreadData();});
    static AtomicLong[] _immotal = new AtomicLong[AllocPoint.END];

    static {
        for (int i=0; i<_immotal.length; i++) {
            _immotal[i] = new AtomicLong();
        }
    }
    
    static class ThreadData {
        @SuppressWarnings("unchecked")
        Deque<ByteBuffer>[] buffers = new ArrayDeque[32 - Integer.numberOfLeadingZeros(MAX_CACHE_BLOCK_SIZE-1) + 1];
        long allocated = 0;
        long pooled = 0;
        Map<Long, Exception> traces;;
        
        ThreadData() {
            for (int i=0; i<this.buffers.length; i++) {
                this.buffers[i] = new ArrayDeque<ByteBuffer>();
            }
        }
    }
    
    public static void setTrace(boolean value) {
        _isTraceEnabled = value;
    }
    
    private static ThreadData getThreadData() {
        ThreadData result = _local.get();
        return result;
    }
    
    public static ByteBuffer alloc(int size) {
        _log.trace("alloc {}", size);
        ByteBuffer result = LatencyDetector.run(_log, "alloc0", ()->{
            return alloc0(size); 
        });
        return result;
    }
    
    private static ByteBuffer alloc0(int size) {
        ByteBuffer result = null;
        ThreadData local = getThreadData();
        int index = 32 - Integer.numberOfLeadingZeros(size-1);
        
        // allocate aligned memory if it is less than MAX_CACHE_SIZE
        
        if (size <= MAX_CACHE_BLOCK_SIZE) {
            Deque<ByteBuffer> q = local.buffers[index];
            result = q.pollLast();
            if (result != null) {
                local.pooled -= result.capacity();
            }
        }
        
        // allocate whatever it is if it is too big
        
        if (result == null) {
            int roundedSize = 1 << index;
            _log.trace("ByteBuffer.allocateDirect {}", roundedSize);
            result = ByteBuffer.allocateDirect(roundedSize);
            result.order(ByteOrder.nativeOrder());
        }
        
        // track the allocation
        
        if (_isDebugEnabled) {
            local.allocated += result.capacity();
        }
        if (_isTraceEnabled) {
            if (local.traces == null) {
                local.traces = new HashMap<>();
            }
            local.traces.put(UberUtil.getAddress(result), new Exception());
        }
        
        // done
        
        return result;
    }
    
    public static void free(ByteBuffer buf) {
        ThreadData local = getThreadData();
        if (_isDebugEnabled) {
            local.allocated -= buf.capacity();
        }
        if (_isTraceEnabled) {
            if (local.traces == null) {
                local.traces = new HashMap<>();
            }
            local.traces.remove(UberUtil.getAddress(buf));
        }
        if (buf.capacity() <= MAX_CACHE_BLOCK_SIZE) {
            if ((buf.capacity() + local.pooled) <= MAX_CACHE_SIZE_PER_THREAD) {
                int index = 32 - Integer.numberOfLeadingZeros(buf.capacity()-1);
                Deque<ByteBuffer> q = local.buffers[index];
                buf.clear();
                q.offerLast(buf);
                local.pooled += buf.capacity();
                buf = null;
            }
        }
        if (buf != null) {
            _log.trace("ByteBuffer free {}", buf.capacity());
            Unsafe.free(buf);
        }
    }

    public static void threadEnd() {
        _local.remove();
    }
    
    public static void report() {
        _log.info("allocated: {}", getAllocated());
        _log.info("pooled: {}" , getPooled());
    }
    
    public static long getAllocated() {
        long result = 0;
        for (Thread i:Thread.getAllStackTraces().keySet()) {
            ThreadData ii = getValueForThread(_local, i);
            if (ii != null) {
                result += ii.allocated;
            }
        }
        return result;
    }

    public static long getPooled() {
        long result = 0;
        for (Thread i:Thread.getAllStackTraces().keySet()) {
            ThreadData ii = getValueForThread(_local, i);
            if (ii != null) {
                result += ii.pooled;
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    static <E> E getValueForThread(ThreadLocal<?> threadLocal, Thread thread) {
        try {
            Method getMap = ThreadLocal.class.getDeclaredMethod("getMap", new Class<?>[]{Thread.class});
            getMap.setAccessible(true);
            Object map = getMap.invoke(threadLocal, thread);
            if (map == null) {
                return null;
            }
            Class<?> clazz = map.getClass();
            Method getEntry = clazz.getDeclaredMethod("getEntry", new Class<?>[]{ThreadLocal.class});
            getEntry.setAccessible(true);
            Object entry = getEntry.invoke(map, threadLocal);
            if (entry == null) {
                return null;
            }
            Field value = entry.getClass().getDeclaredField("value");
            value.setAccessible(true);
            return (E)value.get(entry);
        }
        catch (Exception x) {
            x.printStackTrace();
            return null;
        }
    }
    
    public static long getThreadAllocation() {
        if (_isDebugEnabled) {
            return getThreadData().allocated;
        }
        else {
            return 0;
        }
    }
    
    public static Map<Long, Exception> getTrace() {
        return getThreadData().traces;
    }
    
    public static ByteBuffer allocImmortal(int point, int size) {
        _log.trace("allocImmortal {}", size);
        ByteBuffer result = ByteBuffer.allocateDirect(size);
        AtomicLong counter = _immotal[point];
        counter.addAndGet(size);
        return result;
    }
    
    public static ByteBuffer growImmortal(int point, ByteBuffer old, int size) {
        if ((old != null) && old.capacity() >= size) {
            return old;
        }
        ByteBuffer result = allocImmortal(point, size);
        if (old != null) {
            old.flip();
            result.put(old);
            freeImmortal(point, old);
            result.order(old.order());
        }
        return result;
    }
    
    public static void freeImmortal(int point, ByteBuffer buffer) {
        if (buffer == null) {
            return;
        }
        AtomicLong counter = _immotal[point];
        counter.addAndGet(-buffer.capacity());
        _log.trace("freeImmortal {}", buffer.capacity());
        Unsafe.free(buffer);
    }

    public static AtomicLong[] getImmortals() {
        return _immotal;
    }
}
