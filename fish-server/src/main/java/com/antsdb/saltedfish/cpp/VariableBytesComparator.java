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
public final class VariableBytesComparator extends KeyComparator {
	public int compare(long pKeyX, long pKeyY) {
		return compare_(pKeyX, pKeyY);
	}

	public final static int compare_(long xAddr, long yAddr) {
		int xLength = Unsafe.getShortVolatile(xAddr+1);
		int yLength = Unsafe.getShortVolatile(yAddr+1);
		int minLength = Math.min(xLength, yLength);
		for (int i=0; i<=minLength-1; i++) {
			int x = Unsafe.getByteVolatile(xAddr + 4 + i) & 0xff;
			int y = Unsafe.getByteVolatile(yAddr + 4 + i) & 0xff;
			int result = x - y;
			if (result != 0) {
				return result;
			}
		}
		if (xLength > minLength) {
			return 1;
		}
		if (yLength > minLength) {
			return -1;
		}
		return 0;
	}
}
