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

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;

import com.antsdb.saltedfish.charset.Codecs;
import com.antsdb.saltedfish.charset.Decoder;
import com.antsdb.saltedfish.charset.Utf8;

/**
 * 
 * @author wgu0
 */
public final class FishUtf8 {
	public final static int HEADER_SIZE = 4;
	private static Decoder _utf8 = Codecs.UTF8;

	private long addr;
	private int size;
	
	public final static class Scanner implements IntSupplier {
		long p;
		long pEnd;
		
		Scanner(long addr, int size) {
		    this.p = addr;
		    this.pEnd = addr + size;
		}
		
		@Override
		public int getAsInt() {
			if (p >= pEnd) {
				return -1;
			}
			int result = _utf8.get(()-> {return Unsafe.getByte(p++) & 0xff;});
			return result;
		}
	}
	
    public FishUtf8(long addr) {
        int format = Unsafe.getByte(addr);
        if (format != Value.FORMAT_UTF8) {
            throw new IllegalArgumentException();
        }
        this.addr = addr;
        this.size = Unsafe.getInt3(addr+1);
    }
    
    public IntSupplier scan() {
        return new Scanner(addr + HEADER_SIZE, this.size);
    }
    
	public final static long alloc(Heap heap, int size) {
		if (size > 0xffffff) {
			throw new IllegalArgumentException();
		}
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
		if (size > 0xffffff) {
			throw new IllegalArgumentException();
		}
		long pResult = heap.alloc(HEADER_SIZE + size);
		long pData = pResult + 4;
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
		long pResult = heap.alloc(HEADER_SIZE + size, false);;
		AtomicLong pData = new AtomicLong(pResult + HEADER_SIZE);
		Unsafe.putByte(pResult, Value.FORMAT_UTF8);
		Utf8.encode(string, n -> {
			Unsafe.putByte(pData.getAndIncrement(), (byte)n);
		});
		long realSize = pData.get() - pResult - HEADER_SIZE;
		if (realSize > 0xffffff) {
			throw new IllegalArgumentException();
		}
		Unsafe.putInt3(pResult + 1, (int)realSize);
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
        final int size = Unsafe.getInt3(pValue + 1);
        String result = Utf8.decode(pValue + 4, size);
        return result;
    }

	public static int compare(long pX, long pY) {
		IntSupplier scannerX = new FishUtf8(pX).scan();
		IntSupplier scannerY = new FishUtf8(pY).scan();
		for (;;) {
			int x = scannerX.getAsInt();
			int y = scannerY.getAsInt();
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

	public long toUnicode16(Heap heap) {
        long pResult = Unicode16.allocSet(heap, this.size);
        IntSupplier scan = scan();
		int ch;
		int length = 0;
		while ((ch=scan.getAsInt()) >= 0) {
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
		IntSupplier scan = new FishUtf8(pValue).scan();
		int ch;
		while ((ch=scan.getAsInt()) >= 0) {
			buf.append((char)ch);
		}
		return buf.toString();
	}

    public static int getLength(int format, long pValue) {
        return new FishUtf8(pValue).getLength();
    }
    
	public int getLength() {
		IntSupplier scanner = scan();
		int length = 0;
		while (scanner.getAsInt() >= 0) {
			length++;
		}
		return length;
	}

	public int getSize() {
	    return this.size;
	}
}
