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

import com.antsdb.saltedfish.util.BytesUtil;

/**
 * 
 * @author *-xguo0<@
 */
public final class KeyBytes {
	long addr;
	
	public KeyBytes (long addr) {
		if (Unsafe.getByte(addr) != Value.FORMAT_KEY_BYTES) {
			throw new IllegalArgumentException();
		}
		this.addr = addr;
	}
	
	private KeyBytes() {
	}

	public static KeyBytes create(long addr) {
		if (addr == 0) {
			return null;
		}
		return new KeyBytes(addr);
	}
	
	public static KeyBytes alloc(byte[] bytes) {
		int len = bytes.length;
		if (len >= Short.MAX_VALUE) {
			throw new IllegalArgumentException();
		}
		long addr = Unsafe.allocateMemory(len + 4);
		Unsafe.putByte(addr, Value.FORMAT_KEY_BYTES);
		Unsafe.putShort(addr+2, (short)len);
		Unsafe.putBytes(addr + 4, bytes);
		KeyBytes result = new KeyBytes();
		result.addr = addr;
		return result;
	}
	
	public static KeyBytes alloc(Heap heap, int len) {
		if (len >= Short.MAX_VALUE) {
			throw new IllegalArgumentException();
		}
		long addr = heap.alloc(len + 4);
		Unsafe.putByte(addr, Value.FORMAT_KEY_BYTES);
		Unsafe.putShort(addr+2, (short)len);
		KeyBytes result = new KeyBytes();
		result.addr = addr;
		return result;
	}
	
	public static KeyBytes allocSet(Heap heap, byte[] bytes) {
		if (bytes == null) {
			return null;
		}
		KeyBytes result = alloc(heap, bytes.length);
		Unsafe.putBytes(result.addr + 4, bytes);
		return result;
	}

	public static KeyBytes allocSet(Heap heap, long addr, int length) {
		if (addr == 0) {
			return null; 
		}
		KeyBytes result = alloc(heap, length);
		Unsafe.copyMemory(addr, result.addr + 4, length);
		return result;
	}

	public long getAddress() {
		return this.addr;
	}
	
	public short getLength() {
		short length = Unsafe.getShortVolatile(addr+2);
		return length;
	}
	
	public static short getLength(long addr) {
		short length = Unsafe.getShortVolatile(addr+2);
		return length;
	}
	
	public static int getRawSize(long pValue) {
		return getLength(pValue) + 4;
	}
	
	public byte[] get() {
		short length = getLength();
		byte[] bytes = new byte[length];
		Unsafe.getBytes(addr+4, bytes);
		return bytes;
	}

	public static byte[] get(long addr) {
		return create(addr).get();
	}
	
	public void set(long addr, int index, byte value) {
		if (index >= getLength()) {
			throw new IllegalArgumentException();
		}
		Unsafe.putByte(addr + 4 + index, value);
	}
	
	public void set(byte[] bytes) {
		if (bytes.length > getLength()) {
			throw new IllegalArgumentException();
		}
		Unsafe.putShort(addr+2, (short)bytes.length);
		Unsafe.putBytes(addr+4, bytes);
	}
	
	public int compare(KeyBytes that) {
		int xLength = getLength();
		int yLength = that.getLength();
		int minLength = Math.min(xLength, yLength);
		for (int i=0; i<minLength; i++) {
			int btx = Unsafe.getByte(this.addr + 4 + i) & 0xff;
			int bty = Unsafe.getByte(that.addr + 4 + i) & 0xff;
			int result = btx - bty;
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

	public String toString() {
		String s = BytesUtil.toCompactHex(get());
		return s;
	}

	public static Object toString(long pKey) {
		return create(pKey).toString();
	}

	public void resize(int length) {
		if (length > getLength()) {
			throw new IllegalArgumentException("size can only shrink");
		}
		Unsafe.putShort(this.addr + 2, (short)length);
	}

	/**
	 * get the rowid suffix if this is non unique index
	 * @return -1 if the rowid suffix doesnt exist
	 */
	public long getSuffix() {
		int len = getLength();
		int suffix = Unsafe.getByte(this.addr + 1) & 0xff;
		if (suffix == 0) {
			return 0;
		}
		int pos = suffix >>> 4;
		long result;
		if (pos > 8) {
			long lastLastLong = Unsafe.getLong(this.addr + 4 + len - 16);
			long lastLong = Unsafe.getLong(this.addr + 4 + len - 8);
			int bits = (16 - pos) * 8;
			result = lastLastLong << bits;
			result = result | (lastLong  >>> (64 - bits));
		}
		else {
			long lastLong = Unsafe.getLong(this.addr + 4 + len - 8);
			int bits = (8 - pos) * 8;
			result = lastLong << bits;
		}
		return result;
	}

	public void setSuffixPostion(int posRowid, int sizeRowid) {
		int len = getLength();
		int posFromTail = len - posRowid;
		int suffix = posFromTail << 4 | sizeRowid;
		Unsafe.putByte(this.addr + 1, (byte)suffix);
	}
}
