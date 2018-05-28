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
 * UTF-8 based string
 *  
 * @author wgu0
 */
public class Unicode8 {
	final static int HEADER_SIZE = 4;
	
	public final static String get(Heap heap, long addr) {
		int format = Unsafe.getByte(addr);
		if (format != Value.FORMAT_UTF8) {
			throw new IllegalArgumentException();
		}
		int length = getLength(format, addr);
		char[] chars = new char[length];
		for (int i=0; i<length; i++) {
			char ch = (char)Unsafe.getShort(addr + 1 + 4 + i*2);
			chars[i] = ch;
		}
		return new String(chars);
	}
	
	public final static long allocSet(Heap heap, String s) {
		long addr = heap.alloc(1 + 4 + s.length() * 2);
		Unsafe.putByte(addr, Value.FORMAT_UNICODE16);
		Unsafe.putInt(addr+1, s.length());
		for (int i=0; i<s.length(); i++) {
			Unsafe.putShort(addr + 1 + 4 + i*2, (short)s.charAt(i));
		}
		return addr;
	}

	public final static boolean isEmpty(long addr) {
		int length = Unsafe.getInt(addr+1);
		return length == 0;
	}
	
	final static int getLength(int format, long addr) {
		int length = Unsafe.getInt3(addr+1);
		return length;
	}

	final static char getCharAt(int format, int length, long addr, int idx) {
		if (idx >= length) {
			throw new IllegalArgumentException();
		}
		char ch = (char) Unsafe.getShort(addr + HEADER_SIZE + idx * 2); 
		return ch;
	}

	final static long concat(Heap heap, byte formatX, long addrX, byte formatY, long addrY) {
		int lengthX = Unicode16.getLength(formatX, addrX);
		int lengthY = Unicode16.getLength(formatY, addrY);
		int length = lengthX + lengthY;
		long addr = heap.alloc(1 + 4 + length * 2);
		Unsafe.putByte(addr, Value.FORMAT_UNICODE16);
		Unsafe.putInt(addr+1, length);
		Unsafe.copyMemory(addrX + 5, addr + 1 + 4, lengthX * 2);
		Unsafe.copyMemory(addrY + 5, addr + 1 + 4 + lengthX * 2, lengthY * 2);
		return addr;
	}

	final static long toBytes(Heap heap, byte format, long addr) {
		int length = getLength(format, addr);
		return Bytes.allocSet(heap, addr + 5, length * 2);
	}

}
