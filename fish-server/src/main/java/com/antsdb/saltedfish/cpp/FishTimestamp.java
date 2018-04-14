/*-------------------------------------------------------------------------------------------------
 _______ __   _ _______ _______ ______  ______
 |_____| | \  |    |    |______ |     \ |_____]
 |     | |  \_|    |    ______| |_____/ |_____]

 Copyright (c) 2016, antsdb.com and/or its affiliates. All rights reserved. *-xguo0<@

 This program is free software: you can redistribute it and/or modify it under the terms of the
 GNU Affero General Public License, version 3, as published by the Free Software Foundation.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/agpl-3.0.txt>
-------------------------------------------------------------------------------------------------*/
package com.antsdb.saltedfish.cpp;

import java.sql.Timestamp;

public class FishTimestamp {
	static final int SIZE = 9;
	
	public final static Timestamp get(Heap heap, long address) {
		long lvalue = Unsafe.getLong(address+1);
		Timestamp value = new Timestamp(lvalue);
		return value;
	}

	public final static long getEpochMillisecond(Heap heap, long address) {
		long epoch = Unsafe.getLong(address+1);
		return epoch;
	}
	
	public static final long allocSet(Heap heap, Timestamp value) {
		long address = heap.alloc(SIZE);
		set(heap, address, value);
		return address;
	}

	public static final long allocSet(Heap heap, long value) {
		long address = heap.alloc(SIZE);
		set(heap, address, value);
		return address;
	}

	public static final void set(Heap heap, long address, Timestamp value) {
		Unsafe.putByte(address, Value.FORMAT_TIMESTAMP);
		Unsafe.putLong(address+1, value.getTime());
	}

	public static final void set(Heap heap, long address, long value) {
		Unsafe.putByte(address, Value.FORMAT_TIMESTAMP);
		Unsafe.putLong(address+1, value);
	}

	public static int compare(long xAddr, long yAddr) {
		long x = Unsafe.getLong(xAddr + 1);
		long y = Unsafe.getLong(yAddr + 1);
		return Long.compare(x, y);
	}
	
	public final static int getSize(long pValue) {
		return SIZE;
	}

	public static long add(Heap heap, long pX, long pY) {
		long x = getEpochMillisecond(heap, pX);
		long y = getEpochMillisecond(heap, pY);
		long z = x + y;
		return allocSet(heap, z);
	}

    public static boolean isAllZero(long pValue) {
        long value = Unsafe.getLong(pValue+1);
        return value == Long.MIN_VALUE;
    }

}
