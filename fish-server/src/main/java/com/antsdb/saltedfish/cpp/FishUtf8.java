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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;

import com.antsdb.saltedfish.charset.Utf8;

/**
 * 
 * @author wgu0
 */
public final class FishUtf8 {
	public final static int HEADER_SIZE = 4;

	public final static class Scanner implements IntSupplier {
		long p;
		long pEnd;
		
		@Override
		public int getAsInt() {
			if (p >= pEnd) {
				return -1;
			}
			int ch = Unsafe.getByte(p) & 0xff;
			this.p++;
			return ch;
		}
		
		public int getNext() {
			int ch = Utf8.get(this);
			return ch;
		}
	}
	
	public final static long alloc(Heap heap, int size) {
		long pResult = heap.alloc(HEADER_SIZE + size);;
		Unsafe.putByte(pResult, Value.FORMAT_UTF8);
		Unsafe.putInt3(pResult + 1, size);
		return pResult;
	}
	
	/**
	 * 
	 * @param heap
	 * @param pValue nullable, pointer to a utf 8 bytes
	 * @param size number of bytes taken by the utf8 string, not the string length
	 * @return
	 */
	public final static long allocSet(Heap heap, long pValue, int size) {
		if (pValue == 0) {
			return 0;
		}
		long pResult = heap.alloc(HEADER_SIZE + size);;
		long pData = pResult + 4;;
		Unsafe.putByte(pResult, Value.FORMAT_UTF8);
		Unsafe.putInt3(pResult + 1, size);
		Unsafe.copyMemory(pValue, pData, size);
		return pResult;
	}
	
	public static long allocSet(Heap heap, String string) {
		if (string == null) {
			return 0;
		}
		int size = string.length() * 3;
		long pResult = heap.alloc(HEADER_SIZE + size);;
		AtomicLong pData = new AtomicLong(pResult + HEADER_SIZE);
		Unsafe.putByte(pResult, Value.FORMAT_UTF8);
		Utf8.encode(string, n -> {
			Unsafe.putByte(pData.getAndIncrement(), (byte)n);
		});
		Unsafe.putInt3(pResult + 1, (int)(pData.get() - pResult - HEADER_SIZE));
		return pResult;
	}
	
	/**
	 * number of bytes taken by the string excluding header
	 * @param format
	 * @param pValue
	 * @return
	 */
	public static int getStringSize(int format, long pValue) {
		int size = Unsafe.getInt3(pValue + 1);
		return size;
	}
	
	/**
	 * number of bytes taken by the string including header, not the string length
	 * 
	 * @param pValue
	 * @return
	 */
	public static int getSize(int format, long pValue) {
		int size = Unsafe.getInt3(pValue + 1);
		size = size + HEADER_SIZE; 
		return size;
	}
	
	public static String get(long pValue) {
		AtomicInteger idx = new AtomicInteger();
		int size = Unsafe.getInt3(pValue + 1);
		long pData = pValue + 4;
		String s = Utf8.decode(() -> {
			int pos = idx.getAndIncrement();
			if (pos >= size) {
				return -1;
			}
			int ch = Unsafe.getByte(pData + pos);
			return ch;
		});
		return s;
	}

	public static Scanner scan(long pValue) {
		Scanner scanner = new Scanner();
		int size = Unsafe.getInt3(pValue + 1);
		scanner.p = pValue + 4;
		scanner.pEnd = scanner.p + size;
		return scanner;
	}

	public static int compare(long pX, long pY) {
		FishUtf8.Scanner scannerX = FishUtf8.scan(pX);
		FishUtf8.Scanner scannerY = FishUtf8.scan(pY);
		for (;;) {
			int x = scannerX.getNext();
			int y = scannerY.getNext();
			int result = x - y;
			if (result != 0) {
				return result;
			}
			if ((x == -1) && (y == -1)) {
				break;
			}
		}
		return 0;
	}

	public static long toUnicode16(Heap heap, long pValue) {
		int size = getSize(Value.FORMAT_UTF8, pValue);
		long pResult = Unicode16.allocSet(heap, size);
		Scanner scanner = new Scanner();
		scanner.p = pValue + 4;
		scanner.pEnd = scanner.p + size - 4;
		int ch;
		int length = 0;
		while ((ch=scanner.getNext()) >= 0) {
			Unsafe.putShort(pResult + 4 + length * 2, (short)ch);
			length++;
		}
		Unsafe.putInt3(pResult + 1, length);
		return pResult;
	}

	public static byte[] getBytes(long pValue) {
		if (pValue == 0) {
			return null;
		}
		if (Value.getFormat(null, pValue) != Value.FORMAT_UTF8) {
			throw new IllegalArgumentException();
		}
		int size = Unsafe.getInt3(pValue + 1);
		byte[] bytes = new byte[size];
		for (int i=0; i<size; i++) {
			bytes[i] = Unsafe.getByte(pValue + HEADER_SIZE + i);
		}
		return bytes;
	}

	public static long toBytes(Heap heap, byte format, long pValue) {
		if (pValue == 0) {
			return 0;
		}
		if (format != Value.FORMAT_UTF8) {
			throw new IllegalArgumentException();
		}
		int size = Unsafe.getInt3(pValue + 1);
		return Bytes.allocSet(heap, pValue + HEADER_SIZE, size);
	}

	public static String getString(long pValue) {
		StringBuilder buf = new StringBuilder();
		int size = getSize(Value.FORMAT_UTF8, pValue);
		Scanner scanner = new Scanner();
		scanner.p = pValue + 4;
		scanner.pEnd = scanner.p + size - 4;
		int ch;
		while ((ch=scanner.getNext()) >= 0) {
			buf.append((char)ch);
		}
		return buf.toString();
	}

	public static int getLength(int format, long pValue) {
		Scanner scanner = scan(pValue);
		int length = 0;
		while (scanner.getNext() >= 0) {
			length++;
		}
		return length;
	}

}
