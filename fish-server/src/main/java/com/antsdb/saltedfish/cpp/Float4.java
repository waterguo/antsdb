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

public final class Float4 extends BrutalMemoryObject {
    public Float4(long addr) {
        super(addr);
    }

    public static final long allocSet(Heap heap, float value) {
        long address = heap.alloc(5);
        set(address, value);
        return address;
    }

    public static final void set(long address, float value) {
        Unsafe.putByte(address, Value.FORMAT_FLOAT4);
        Unsafe.putFloat(address + 1, value);
    }

    public static final float get(Heap heap, long address) {
        float n = Unsafe.getFloat(address + 1);
        return n;
    }

    public final static int getSize(long pValue) {
        return 5;
    }

    public static int compare(long addrx, byte typey, long addry) {
        double x = get(null, addrx);
        double y;
        switch (typey) {
        case Value.FORMAT_FLOAT4:
            y = get(null, addry);
            break;
        case Value.FORMAT_DECIMAL:
            y = FishDecimal.get(null, addry).doubleValue();
            break;
        case Value.FORMAT_FAST_DECIMAL:
            y = FastDecimal.get(null, addry).doubleValue();
            break;
        case Value.FORMAT_INT8:
            y = Int8.get(null, addry);
            break;
        case Value.FORMAT_INT4:
            y = Int4.get(addry);
            break;
        default:
            throw new IllegalArgumentException();
        }
        return Double.compare(x, y);
    }

    @Override
    public int getByteSize() {
        return getSize(this.addr);
    }

    @Override
    public int getFormat() {
        return Value.FORMAT_FLOAT4;
    }

}
