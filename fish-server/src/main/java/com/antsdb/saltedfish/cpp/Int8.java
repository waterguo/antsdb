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

public class Int8 {
	//final static Heap _constants = new BluntHeap(1024);
	//public final static long ZERO = allocSet(_constants, 0);
	
	public final static long get(Heap heap, long address) {
		long value = Unsafe.getLong(address+1);
		return value;
	}

	public static final long allocSet(Heap heap, long value) {
		long address = heap.alloc(9);
		set(heap, address, value);
		return address;
	}

	public static final void set(Heap heap, long address, long value) {
		if (address == 0) {
			throw new IllegalArgumentException();
		}
		Unsafe.putByte(address, Value.FORMAT_INT8);
		Unsafe.putLong(address+1, value);
	}

	final static int compare(long addr1, byte type2, long addr2) {
		long x = get(null, addr1);
		long y;
		switch (type2) {
		case Value.FORMAT_INT8:
			y = get(null, addr2);
			break;
		case Value.FORMAT_INT4:
			y = Int4.get(addr2);
			break;
		default:
			throw new IllegalArgumentException();
		}
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
	}

	public static int getSize() {
		return 9;
	}

	public static long abs(Heap heap, long pValue) {
		long value = get(heap, pValue);
		if (value >= 0) {
			return pValue;
		}
		return allocSet(heap, -value);
	}

	public static long negate(Heap heap, long pValue) {
		long value = get(heap, pValue);
		return allocSet(heap, -value);
	}
}
