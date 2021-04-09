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

import java.sql.Date;;

public final class FishDate extends BrutalMemoryObject {
    
    public FishDate(long addr) {
        super(addr);
    }

    public final static Date get(Heap heap, long address) {
        long lvalue = Unsafe.getLong(address+1);
        Date value = new Date(lvalue);
        return value;
    }

    public static final long allocSet(Heap heap, Date value) {
        long address = heap.alloc(9);
        set(heap, address, value);
        return address;
    }

    public static final long allocSet(Heap heap, long value) {
        long address = heap.alloc(9);
        set(heap, address, value);
        return address;
    }

    public static int getSize(long pValue) {
        return 9;
    }

    public final static long getEpochMillisecond(Heap heap, long address) {
        long epoch = Unsafe.getLong(address+1);
        return epoch;
    }
    
    public static final void set(Heap heap, long address, Date value) {
        Unsafe.putByte(address, Value.FORMAT_DATE);
        Unsafe.putLong(address+1, value.getTime());
    }

    public static final void set(Heap heap, long address, long value) {
        Unsafe.putByte(address, Value.FORMAT_DATE);
        Unsafe.putLong(address+1, value);
    }

    public static int compare(long xAddr, long yAddr) {
        long x = Unsafe.getLong(xAddr + 1);
        long y = Unsafe.getLong(yAddr + 1);
        return Long.compare(x, y);
    }

    @Override
    public int getByteSize() {
        return 9;
    }

    @Override
    public int getFormat() {
        return Value.FORMAT_DATE;
    }
}
