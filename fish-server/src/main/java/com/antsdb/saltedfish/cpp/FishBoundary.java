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

/**
 * boundary is a combination of a key and inclusive indicator. it is used to simplify range calculation
 * 
 * @author wgu0
 */
public final class FishBoundary {
	long p;
	
	public FishBoundary(long pValue) {
		if (Value.getFormat(null, pValue) != Value.FORMAT_BOUNDARY) {
			throw new IllegalArgumentException();
		}
		this.p = pValue;
	}
	
	public boolean isInclusive() {
		return (Unsafe.getByte(p+1) == 1) ? true : false;
	}
	
	public byte[] getKey() {
		long pKey = getKeyAddress();
		byte[] key = Bytes.get(null, pKey);
		return key;
	}
	
	public long getKeyAddress() {
		return Unsafe.getLong(p+2);
	}
	
	public static FishBoundary create(long p) {
		if (p == 0) {
			return null;
		}
		return new FishBoundary(p);
	}
	
	/**
	 * !!!! it only stores the address of key
	 * 
	 * @param heap
	 * @param inclusive
	 * @param pKey
	 * @return
	 */
	public static long alloc(Heap heap, boolean inclusive, long pKey) {
		long p = heap.alloc(8 + 1 + 1);
		Unsafe.putByte(p, Value.FORMAT_BOUNDARY);
		Unsafe.putByte(p+1, (byte)(inclusive ? 1 : 0));
		Unsafe.putLong(p+2, pKey);
		return p;
	}

	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		if (this.p == 0) {
			return "NULL";
		}
		buf.append("inclusive=");
		buf.append(isInclusive());
		buf.append(" ");
		buf.append("key=");
		buf.append(Bytes.toString(getKeyAddress()));
		return buf.toString();
	}
}
