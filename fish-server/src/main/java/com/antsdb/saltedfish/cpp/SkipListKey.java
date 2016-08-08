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
 * 
 * @author wgu0
 */
public class SkipListKey {
	public final static long allocSet(byte[] bytes) {
		if (bytes == null) {
			return 0;
		}
		int length = bytes.length;
		int allocLength = length + 2;
		if (length > Short.MAX_VALUE) {
			throw new RuntimeException("key exceeds 32767 bytes");
		}
		long p = Unsafe.allocateMemory(allocLength);
		Unsafe.putShort(p, (short)length);
		for (int i=0; i<bytes.length; i++) {
			Unsafe.putByte(p + 2 + i, bytes[i]);
		}
		return p;
	}
}
