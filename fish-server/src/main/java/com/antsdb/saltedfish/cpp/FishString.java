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
 * umbrella class for all string like data
 * 
 * @author xinyi
 *
 */
public final class FishString {
	public final static boolean isString(long addr) {
		if (addr == 0) {
			return false;
		}
		int type = Value.getType(null, addr);
		return type == Value.TYPE_STRING;
	}
	
	final static boolean isString_(long addr) {
		int type = Unsafe.getByte(addr);
		switch (type) {
		case Value.FORMAT_UNICODE16:
			return true;
		default:
			return false;
		}
	}
	
	public final static boolean isEmpty(long addr) {
		if (addr == 0) {
			return false;
		}
		int format = Value.getFormat(null, addr);
		if (format == Value.FORMAT_UNICODE16) {
			return Unicode16.getLength(format, addr) == 0;
		}
		else if (format == Value.FORMAT_UTF8) {
			return FishUtf8.getStringSize(Value.FORMAT_UTF8, addr) == 0;
		}
		else {
			throw new IllegalArgumentException();
		}
	}

	final static boolean equals(long xAddr, long yAddr) {
		return compare(xAddr, yAddr) == 0;
	}

	final static int compare(long xAddr, long yAddr) {
		int xFormat = Value.getFormat(null, xAddr);
		int yFormat = Value.getFormat(null, yAddr);
		if (xFormat == Value.FORMAT_UNICODE16) {
			if (yFormat == Value.FORMAT_UNICODE16) {
				return Unicode16.compare(xAddr, yAddr);
			}
			else {
				return compare_16_8(xAddr, yAddr);
			}
		}
		else if (xFormat == Value.FORMAT_UTF8) {
			if (yFormat == Value.FORMAT_UNICODE16) {
				return -compare_16_8(yAddr, xAddr);
			}
			else {
				return FishUtf8.compare(xAddr, yAddr); 
			}
		}
		else {
			throw new IllegalArgumentException();
		}
	}

	private static int compare_16_8(long pX, long pY) {
		FishUtf8.Scanner scanner = FishUtf8.scan(pY);
		int lengthX = Unicode16.getLength(Value.FORMAT_UNICODE16, pX);
		int idx = 0;
		for (;;) {
			int x = (idx < lengthX) ? Unicode16.getCharAt(Value.FORMAT_UNICODE16, lengthX, pX, idx++) : -1;
			int y = scanner.getNext();
			int result = x - y;
			if (result != 0) {
				return result;
			}
			break;
		}
		return 0;
	}

	public final static long concat(Heap heap, long addrX, long addrY) {
		if (addrX == 0) {
			return addrY;
		}
		if (addrY == 0) {
			return addrX;
		}
		byte formatX = Value.getFormat(heap, addrX);
		byte formatY = Value.getFormat(heap, addrY);
		if (formatX == Value.FORMAT_UTF8) {
			addrX = FishUtf8.toUnicode16(heap, addrX);
		}
		if (formatY == Value.FORMAT_UTF8) {
			addrY = FishUtf8.toUnicode16(heap, addrY);
		}
		return Unicode16.concat(heap, formatX, addrX, formatY, addrY);
	}

	public static int getLength(long pValue) {
		int len = Unicode16.getLength(Value.FORMAT_UNICODE16, pValue);
		return len;
	}

	public static char charAt(int format, int length, long pValue, int idx) {
		if (format == Value.FORMAT_UNICODE16) {
			return Unicode16.getCharAt(format, length, pValue, idx);
		}
		else {
			throw new IllegalArgumentException();
		}
	}
}
