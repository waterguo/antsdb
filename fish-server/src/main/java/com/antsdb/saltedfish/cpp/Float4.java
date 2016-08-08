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

public class Float4 {
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

}
