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

import java.math.BigInteger;

public final class BigInt extends BrutalMemoryObject {
    public BigInt(long addr) {
        super(addr);
    }

    public final static BigInteger get(Heap heap, long addr) {
        int type = Unsafe.getByte(addr);
        if (type != Value.FORMAT_BIGINT) {
            throw new IllegalArgumentException();
        }
        int length = Unsafe.getByte(addr+1);
        byte[] bytes = new byte[length];
        Unsafe.getBytes(addr+2, bytes);
        BigInteger value = new BigInteger(bytes);
        return value;
    }

    public final static long allocSet(Heap heap, BigInteger value) {
        byte[] bytes = value.toByteArray();
        long address = heap.alloc(bytes.length + 2);
        set(heap, address, bytes);
        return address;
    }

    final static void set(Heap heap, long address, byte[] bytes) {
        Unsafe.putByte(address, Value.FORMAT_BIGINT);
        Unsafe.putByte(address+1, (byte)bytes.length);
        Unsafe.putBytes(address+2, bytes);
    }

    public final static int getSize(long pValue) {
        int size = Unsafe.getByte(pValue+1);
        size += 2;
        return size;
    }

    @Override
    public int getByteSize() {
        return getSize(this.addr);
    }

    @Override
    public int getFormat() {
        return Value.FORMAT_BIGINT;
    }

    public static int compare(long addr1, byte type2, long addr2) {
        BigInteger x = get(null, addr1);
        BigInteger y;
        switch (type2) {
        case Value.FORMAT_BIGINT:
            y = get(null, addr2);
            break;
        case Value.FORMAT_INT8:
            y = BigInteger.valueOf(Int8.get(null, addr2));
            break;
        case Value.FORMAT_INT4:
            y = BigInteger.valueOf(Int4.get(addr2));
            break;
        default:
            throw new IllegalArgumentException();
        }
        return x.compareTo(y);
    }
}
