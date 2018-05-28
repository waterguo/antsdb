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

/**
 * 
 * @author wgu0
 */
public final class VariableLengthIntegerComparator extends KeyComparator {
	@Override
	public int compare(long xAddr, long yAddr) {
		return compare_(xAddr, yAddr);
	}
	
	public static int compare_(long xAddr, long yAddr) {
		int xLength = Unsafe.getShort(xAddr+1);
		int yLength = Unsafe.getShort(yAddr+1);
		int minLength = Math.min(xLength, yLength);
		for (int i=0; i<=(minLength-1)/4; i++) {
			int x = Unsafe.getInt(xAddr + 4 + i * 4);
			int y = Unsafe.getInt(yAddr + 4 + i * 4);
			int result = Integer.compareUnsigned(x, y);
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
