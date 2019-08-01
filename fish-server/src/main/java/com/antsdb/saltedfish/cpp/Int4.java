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

public final class Int4 extends BrutalMemoryObject {
    
    public Int4(long addr) {
        super(addr);
    }

    public static final int get(long address) {
        int type = Unsafe.getByte(address);
        if (type != Value.FORMAT_INT4) {
            throw new IllegalArgumentException();
        }
        int n = Unsafe.getInt(address + 1);
        return n;
    }
    
    public static final long allocSet(Heap heap, int value) {
        long address = heap.alloc(5);
        set(address, value);
        return address;
    }

    public static final void set(long address, int value) {
        Unsafe.putByte(address, Value.FORMAT_INT4);
        Unsafe.putInt(address + 1, value);
    }

    public static final int get(Heap heap, long address) {
        int n = Unsafe.getInt(address + 1);
        return n;
    }
    
    public static final void set(FlexibleHeap heap, long address, int value) {
        Unsafe.putInt(address, value);
    }
    
    public static final int get(Heap2 heap, long address) {
        int block = (int)(address >> 32);
        int pos = (int)address;
        ByteBuffer buf = heap.buffers.get(block);
        int value = buf.getInt(pos);
        return value;
    }

    public static final void set(Heap2 heap, long address, int value) {
        int block = (int)(address >> 32);
        int pos = (int)address;
        ByteBuffer buf = heap.buffers.get(block);
        buf.putInt(pos, value);
    }

    final static int compare(long addr1, byte type2, long addr2) {
        int x = get(addr1);
        int y = get(addr2);
        if (type2 != Value.FORMAT_INT4) {
            throw new IllegalArgumentException();
        }
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    public final static int getSize() {
        return 5;
    }

    public static long abs(Heap heap, long pValue) {
        int value = get(heap, pValue);
        if (value >= 0) {
            return pValue;
        }
        return allocSet(heap, -value);
    }

    public static long negate(Heap heap, long pValue) {
        int value = get(heap, pValue);
        return allocSet(heap, -value);
    }

    @Override
    public int getByteSize() {
        return getSize();
    }

    @Override
    public int getFormat() {
        return Value.FORMAT_INT4;
    }

}
