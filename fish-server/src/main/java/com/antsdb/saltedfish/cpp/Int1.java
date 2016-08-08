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

public class Int1 {

	public final static byte get(FlexibleHeap heap, long address) {
		byte value = Unsafe.getByte(address);
		return value;
	}

	public static final long allocSet(FlexibleHeap heap, byte value) {
		long address = heap.alloc(1);
		set(heap, address, value);
		return address;
	}

	public static final void set(FlexibleHeap heap, long address, byte value) {
		Unsafe.putByte(address, value);
	}

}
