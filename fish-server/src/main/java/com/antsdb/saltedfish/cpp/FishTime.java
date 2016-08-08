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

import java.sql.Time;

public final class FishTime {
	public final static Time get(Heap heap, long address) {
		long lvalue = Unsafe.getLong(address+1);
		Time value = new Time(lvalue);
		return value;
	}

	public static final long allocSet(Heap heap, Time value) {
		long address = heap.alloc(9);
		set(heap, address, value);
		return address;
	}

	public static final long allocSet(Heap heap, long value) {
		long address = heap.alloc(9);
		set(heap, address, value);
		return address;
	}

	public static final void set(Heap heap, long address, Time value) {
		Unsafe.putByte(address, Value.FORMAT_TIME);
		Unsafe.putLong(address+1, value.getTime());
	}

	public static final void set(Heap heap, long address, long value) {
		Unsafe.putByte(address, Value.FORMAT_TIME);
		Unsafe.putLong(address+1, value);
	}

}
