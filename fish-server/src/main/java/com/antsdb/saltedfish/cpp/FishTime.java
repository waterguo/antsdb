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

import java.time.Duration;

public final class FishTime extends BrutalMemoryObject {
    static final int SIZE = 9;
    
    public FishTime(long addr) {
        super(addr);
    }

    /*
     * use duration here because java.sql.Time only goes up to 23:59:59 while mysql time can go up to 838:59:59
     */
    public final static Duration get(Heap heap, long address) {
        long lvalue = Unsafe.getLong(address+1);
        Duration value = Duration.ofMillis(lvalue);
        return value;
    }

    public static final long allocSet(Heap heap, Duration value) {
        long address = heap.alloc(9);
        set(heap, address, value);
        return address;
    }

    /**
     * 
     * @param heap
     * @param value milliseconds
     * @return
     */
    public static final long allocSet(Heap heap, long value) {
        long address = heap.alloc(9);
        set(heap, address, value);
        return address;
    }

    public static final void set(Heap heap, long address, Duration value) {
        Unsafe.putByte(address, Value.FORMAT_TIME);
        Unsafe.putLong(address+1, value.toMillis());
    }

    /**
     * 
     * @param heap
     * @param address
     * @param value milliseconds
     */
    public static final void set(Heap heap, long address, long value) {
        Unsafe.putByte(address, Value.FORMAT_TIME);
        Unsafe.putLong(address+1, value);
    }

    public static int getSize(long pValue) {
        return SIZE;
    }

    @Override
    public int getByteSize() {
        return SIZE;
    }

    @Override
    public int getFormat() {
        return Value.FORMAT_TIME;
    }

}
