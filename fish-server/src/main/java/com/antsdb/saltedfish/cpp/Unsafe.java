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
import java.nio.ByteBuffer;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

@SuppressWarnings("restriction")
public final class Unsafe {
    static Logger _log = UberUtil.getThisLogger(); 
    final static sun.misc.Unsafe unsafe = UberUtil.getUnsafe();
    
    static private long ix(long addr) {
        // line below is required for ppc
        return (addr & Long.MAX_VALUE);
    }

    public final static void putByte(long addr, byte value) {
        unsafe.putByte(ix(addr), value);
    }
    
    public final static void putByteVolatile(long addr, byte value) {
        unsafe.putByteVolatile(null, ix(addr), value);
    }
    
    public final static void getBytes(long addr, byte[] bytes) {
        for (int i=0; i<bytes.length; i++) {
            bytes[i] = getByte(addr + i);
        }
    }
    
    public final static void putBytes(long addr, byte[] bytes) {
        for (int i=0; i<bytes.length; i++) {
            putByte(addr + i, bytes[i]);
        }
    }
    
    /** get 3 bytes integer */
    public final static int getInt3(long addr) {
        int value = (getByte(addr+2) & 0xff) << 16;
        value = value | (getShort(addr) & 0xffff);
        return value;
    }
    
    public final static int getInt3Volatile(long addr) {
        int value = (getByteVolatile(addr+2) & 0xff) << 16;
        value = value | (getShortVolatile(addr) & 0xffff);
        return value;
    }
    
    /** 3 bytes integer */
    public final static void putInt3(long addr, int value) {
        putShort(addr, (short)value);
        putByte(addr+2, (byte)(value >> 16));
    }

    public final static void putInt3Volatile(long addr, int value) {
        putShortVolatile(addr, (short)value);
        putByteVolatile(addr+2, (byte)(value >> 16));
    }

    public final static void putInt(long addr, int value) {
        unsafe.putInt(ix(addr), value);
    }

    public final static void putIntVolatile(long addr, int value) {
        unsafe.putIntVolatile(null, ix(addr), value);
    }

    public final static void putOrderedLong(long addr, long value) {
        unsafe.putOrderedLong(null, ix(addr), value);
    }

    public final static void putLong(long addr, long value) {
        unsafe.putLong(ix(addr), value);
    }

    public final static void putLongVolatile(long addr, long value) {
        unsafe.putLongVolatile(null, ix(addr), value);
    }

    public final static byte getByte(long addr) {
        return unsafe.getByte(ix(addr));
    }

    public static byte getByteVolatile(long addr) {
        return unsafe.getByteVolatile(null, ix(addr));
    }
    
    public final static int getInt(long addr) {
        return unsafe.getInt(ix(addr));
    }
    
    public final static int getIntVolatile(long addr) {
        return unsafe.getIntVolatile(null, ix(addr));
    }
    
    public final static long getLong(long addr) {
        return unsafe.getLong(ix(addr));
    }
    
    public final static long getLongVolatile(long addr) {
        return unsafe.getLongVolatile(null, ix(addr));
    }

    public final static void setMemory(long addr, int bytes, byte value) {
        int alignedSize = bytes / 8 * 8;
        int i = 0;
        if (alignedSize > 1) {
            long lv;
            if (value == 0) {
                lv = 0;
            }
            else {
                lv = value & 0xff;
                lv = lv << 56 | lv << 48 | lv << 40 | lv << 32 | lv << 24 | lv << 16 | lv << 8 | lv;
            }
            for (;i < alignedSize; i += 8) {
                Unsafe.putLong(i + addr, lv);
            }
        }
        for (; i < bytes; i++) {
            Unsafe.putByte(i + addr, (byte)value);
        }
    }

    public final static void putUnsignedShort(long addr, int value) {
        unsafe.putShort(ix(addr), (short)value);
    }

    public final static int getUnsignedShort(long addr) {
        int value = unsafe.getShort(ix(addr)) & 0xffff;
        return value;
    }

    public final static short getShort(long addr) {
        short value = unsafe.getShort(ix(addr));
        return value;
    }

    public static short getShortVolatile(long addr) {
        short value = unsafe.getShortVolatile(null, ix(addr));
        return value;
    }

    public final static void putShort(long addr, short value) {
        unsafe.putShort(ix(addr), value);
    }
    
    public final static void putShortVolatile(long addr, short value) {
        unsafe.putShortVolatile(null, ix(addr), value);
    }
    
    public final static void copyMemory(long source, long target, long bytes) {
        unsafe.copyMemory(ix(source), ix(target), bytes);
    }
    
    public final static boolean compareAndSwapLong(Object obj, long offset, long expected, long value) {
        return unsafe.compareAndSwapLong(obj, offset, expected, value);
    }

    public final static boolean compareAndSwapInt(long addr, int expected, int value) {
        return unsafe.compareAndSwapInt(null, ix(addr), expected, value);
    }

    public static long allocateMemory(long size) {
        return unsafe.allocateMemory(size);
    }

    public static boolean compareAndSwapLong(long addr, int expected, long value) {
        return unsafe.compareAndSwapLong(null, ix(addr), expected, value);
    }
    
    public static long objectFieldOffset(Field field) {
        return unsafe.objectFieldOffset(field);
    }

    public static boolean compareAndSwapInt(Object obj, long headoffset, int cmp, int val) {
        return unsafe.compareAndSwapInt(obj, headoffset, cmp, val);
    }
    
    public final static void freeMemory(long p) {
        unsafe.freeMemory(p);
    }

    public static void free(ByteBuffer buf) {
        if (!(buf instanceof sun.nio.ch.DirectBuffer)) {
            throw new IllegalArgumentException();
        }
        sun.misc.Cleaner cleaner = ((sun.nio.ch.DirectBuffer)buf).cleaner();
        cleaner.clean();
    }
    
    public static void unmap(ByteBuffer buf) {
        free(buf);
    }

    public final static void putFloat(long addr, float value) {
        unsafe.putFloat(ix(addr), value);
    }

    public final static float getFloat(long addr) {
        float value = unsafe.getFloat(ix(addr));
        return value;
    }

    public final static void putDouble(long addr, double value) {
        unsafe.putDouble(ix(addr), value);
    }

    public final static double getDouble(long addr) {
        double value = unsafe.getDouble(ix(addr));
        return value;
    }

    public static long getAndAddLong(long addr, long value) {
        return unsafe.getAndAddLong(null, ix(addr), value);
    }

    public static int compare(long px, int sizeX, long py, int sizeY) {
        int minLength = Math.min(sizeX, sizeY);
        for (int i=0; i<minLength; i++) {
            int x = Unsafe.getByte(px + i) & 0xff;
            int y = Unsafe.getByte(py + i) & 0xff;
            int result = x - y;
            if (result != 0) {
                return result;
            }
        }
        if (sizeX > sizeY) {
            return 1;
        }
        else if (sizeX < sizeY) {
            return -1;
        }
        else {
            return 0;
        }
    }

}
