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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import com.antsdb.saltedfish.util.BytesUtil;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * very simple heap that can't grow 
 *  
 * @author xinyi
 */
public class BluntHeap extends Heap implements AutoCloseable {
	static final int DEFAULT_SIZE = 1024 * 1024;
	static final int ALIGNMENT = 4;
    static final int ALIGNMENT_MASK = ALIGNMENT - 1;
	
	ByteBuffer buf;
	long address;
	volatile int capacity;
	AtomicInteger pos = new AtomicInteger();
	
	public BluntHeap () {
		this(DEFAULT_SIZE);
	}
	
	public BluntHeap(ByteBuffer buf) {
		this.buf = buf;
		this.capacity = buf.capacity();
		this.address = UberUtil.getAddress(buf);
	}
	
	public BluntHeap(int capacity) {
		this.buf = MemoryManager.alloc(capacity);
		this.capacity = capacity;
		this.address = UberUtil.getAddress(buf);
	}
	
	public BluntHeap(long address, int capacity) {
		this.address = address;
		this.capacity = capacity;
	}
	
	/**
	 * 
	 * @param size
	 * @return address/pointer of the allocated address
	 */
	public final long alloc(int size) {
		int offset = allocOffset(size);
		return this.address + offset;
	}

	public int allocOffset(int size) {
		int mod = size % ALIGNMENT;
		if (mod != 0) {
			size = size & ~ALIGNMENT_MASK;
			size += ALIGNMENT;
		}
		int result = this.pos.getAndAdd(size);
		if ((result + size) < this.capacity) {
			Unsafe.setMemory(this.address + result, size, (byte)0);
			return result;
		}
		// out of heap
		freeze();
		throw new OutOfHeapMemory();
	}

	public long write(ByteBuffer bytes) {
		int size = bytes.remaining();
		long addr = alloc(size);
		for (int i=0; i<size; i++) {
			byte bt = bytes.get();
			Unsafe.putByte(addr + i, bt);
		}
		return addr;
	}

	public void position(int pos) {
		this.pos.set(pos);
	}
	
	public long position() {
		return this.pos.get();
	}

	public void reset(long pos) {
		position((int)pos);
	}

	public final void putByte(int offset, byte value) {
		Unsafe.putByte(this.address + offset, value);
	}
	
	public void putByteVolatile(int offset, byte value) {
		Unsafe.putByteVolatile(this.address + offset, value);
	}

	public final void putInt(int offset, int value) {
		Unsafe.putInt(this.address + offset, value);
	}

	public final byte getByte(int offset) {
		return Unsafe.getByte(this.address + offset);
	}

	public final byte getByteVolatile(int offset) {
		return Unsafe.getByteVolatile(this.address + offset);
	}

	public final int getInt(int offset) {
		return Unsafe.getInt(this.address + offset);
	}

	public final int getIntVolatile(int offset) {
		return Unsafe.getIntVolatile(this.address + offset);
	}

	public void putIntVolatile(int offset, int value) {
		Unsafe.putIntVolatile(this.address + offset, value);
	}

	public final long getLong(int offset) {
		return Unsafe.getLong(this.address + offset);
	}

	public final long getLongVolatile(int offset) {
		return Unsafe.getLongVolatile(this.address + offset);
	}

	public long getAddress(int offset) {
		return this.address + offset;
	}

	public void putLong(int offset, long value) {
		Unsafe.putLong(this.address + offset, value);
	}

	public void putLongVolatile(int offset, long value) {
		Unsafe.putLongVolatile(this.address + offset, value);
	}

	public final boolean compareAndSwapInt(int offset, int oldValue, int newValue) {
		return Unsafe.compareAndSwapInt(this.address + offset, oldValue, newValue);
	}

	public boolean compareAndSwapLong(int offset, int oldValue, long newValue) {
		return Unsafe.compareAndSwapLong(this.address + offset, oldValue, newValue);
	}

	@Override
	public String toString() {
		int size = (int)position();
		byte[] bytes = new byte[size];
		for (int i=0; i<size; i++) {
			bytes[i] = Unsafe.getByte(this.address + i);
		}
		return BytesUtil.toHex(bytes);
	}

	@Override
	public void close() {
		if (this.buf != null) {
			MemoryManager.free(this.buf);
		}
		this.buf = null;
	}

	public int capacity() {
		return this.capacity;
	}

	/**
	 * prevents writes by filling it up
	 * 
	 * @return the last position before freeze 
	 */
	public int freeze() {
	    for (;;) {
    	    int current = this.pos.get();
    		if (this.pos.compareAndSet(current, this.capacity)) {
    		    return current;
    		}
	    }
	}

	@Override
	public void free() {
		close();
	}

	public boolean isFull() {
		boolean result = this.capacity == this.position();
		return result;
	}

}
