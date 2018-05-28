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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.antsdb.saltedfish.util.UberUtil;

/**
 * a heap that grows exponentially
 *  
 * @author wgu0
 */
public class ExpoHeap {
	static final int DEFAULT_CAPACITY = 128 * 1024 * 1024 - 4096;  
			
	List<ByteBuffer> allocated = new ArrayList<>(32);
	AtomicInteger position = new AtomicInteger();
	volatile int capacity;
	volatile long addr_4k;
	volatile long addr_8k;
	volatile long addr_16k;
	volatile long addr_32k;
	volatile long addr_64k;
	volatile long addr_128k;
	volatile long addr_256k;
	volatile long addr_512k;
	volatile long addr_1m;
	volatile long addr_2m;
	volatile long addr_4m;
	volatile long addr_8m;
	volatile long addr_16m;
	volatile long addr_32m;
	volatile long addr_64m;
	volatile long addr_128m;
	volatile long addr_256m;
	volatile long addr_512m;
	
	public ExpoHeap() {
		// start with 4k capacity
		
		this.capacity = 4 * 1024;
		ByteBuffer buf = MemoryManager.alloc(this.capacity);
		this.allocated.add(buf);
		this.addr_4k = UberUtil.getAddress(buf);
	}
	
	public final int alloc(int size) {
		for (;;) {
			int currentCapacity = capacity;
			int currentPos = this.position.get();
			if (size > (currentCapacity - currentPos)) {
				grow(currentCapacity);
				continue;
			}
			int newPos = currentPos + size;
			this.position.compareAndSet(currentPos, newPos);
			return currentPos;
		}
	}
	
	public void reset(int position) {
		this.position.set(position);
	}

	private void grow(int currentCapacity) {
		synchronized(this) {
			if (this.capacity != currentCapacity) {
				// race condition
				return;
			}
			int delta = 0;
			ByteBuffer buf;
			if (addr_8k == 0) {
				delta = 1024 * 8;
				buf = MemoryManager.alloc(delta);
				this.addr_8k = UberUtil.getAddress(buf) - currentCapacity;
			}
			else if (addr_16k == 0) {
				delta = 1024 * 16;
				buf = MemoryManager.alloc(delta);
				this.addr_16k = UberUtil.getAddress(buf) - currentCapacity;
			}
			else if (addr_32k == 0) {
				delta = 1024 * 32;
				buf = MemoryManager.alloc(delta);
				this.addr_32k = UberUtil.getAddress(buf) - currentCapacity;
			}
			else if (addr_64k == 0) {
				delta = 1024 * 64;
				buf = MemoryManager.alloc(delta);
				this.addr_64k = UberUtil.getAddress(buf) - currentCapacity;
			}
			else if (addr_128k == 0) {
				delta = 1024 * 128;
				buf = MemoryManager.alloc(delta);
				this.addr_128k = UberUtil.getAddress(buf) - currentCapacity;
			}
			else if (addr_256k == 0) {
				delta = 1024 * 256;
				buf = MemoryManager.alloc(delta);
				this.addr_256k = UberUtil.getAddress(buf) - currentCapacity;
			}
			else if (addr_512k == 0) {
				delta = 1024 * 512;
				buf = MemoryManager.alloc(delta);
				this.addr_512k = UberUtil.getAddress(buf) - currentCapacity;
			}
			else if (addr_1m == 0) {
				delta = 1024 * 1024;
				buf = MemoryManager.alloc(delta);
				this.addr_1m = UberUtil.getAddress(buf) - currentCapacity;
			}
			else if (addr_2m == 0) {
				delta = 2 * 1024 * 1024;
				buf = MemoryManager.alloc(delta);
				this.addr_2m = UberUtil.getAddress(buf) - currentCapacity;
			}
			else if (addr_4m == 0) {
				delta = 4 * 1024 * 1024;
				buf = MemoryManager.alloc(delta);
				this.addr_4m = UberUtil.getAddress(buf) - currentCapacity;
			}
			else if (addr_8m == 0) {
				delta = 8 * 1024 * 1024;
				buf = MemoryManager.alloc(delta);
				this.addr_8m = UberUtil.getAddress(buf) - currentCapacity;
			}
			else if (addr_16m == 0) {
				delta = 16 * 1024 * 1024;
				buf = MemoryManager.alloc(delta);
				this.addr_16m = UberUtil.getAddress(buf) - currentCapacity;
			}
			else if (addr_32m == 0) {
				delta = 32 * 1024 * 1024;
				buf = MemoryManager.alloc(delta);
				this.addr_32m = UberUtil.getAddress(buf) - currentCapacity;
			}
			else if (addr_64m == 0) {
				delta = 64 * 1024 * 1024;
				buf = MemoryManager.alloc(delta);
				this.addr_64m = UberUtil.getAddress(buf) - currentCapacity;
			}
			else if (addr_128m == 0) {
				delta = 128 * 1024 * 1024;
				buf = MemoryManager.alloc(delta);
				this.addr_64m = UberUtil.getAddress(buf) - currentCapacity;
			}
			else if (addr_256m == 0) {
				delta = 256 * 1024 * 1024;
				buf = MemoryManager.alloc(delta);
				this.addr_64m = UberUtil.getAddress(buf) - currentCapacity;
			}
			else if (addr_512m == 0) {
				delta = 512 * 1024 * 1024;
				buf = MemoryManager.alloc(delta);
				this.addr_64m = UberUtil.getAddress(buf) - currentCapacity;
			}
			else {
				throw new OutOfHeapMemory();
			}
			this.position.set(capacity);
			this.capacity += delta;
			this.allocated.add(buf);
		}
	}
	
	public final long getAddress(int offset) {
		if (offset < 0) {
			throw new IllegalArgumentException();
		}
		if (offset < 4 * 1024) {
			return this.addr_4k + offset;
		}
		if (offset < 16 * 1024 - 4096) {
			return this.addr_8k + offset;
		}
		if (offset < 32 * 1024 - 4096) {
			return this.addr_16k + offset;
		}
		if (offset < 64 * 1024 - 4096) {
			return this.addr_32k + offset;
		}
		if (offset < 128 * 1024 - 4096) {
			return this.addr_64k + offset;
		}
		if (offset < 256 * 1024 - 4096) {
			return this.addr_128k + offset;
		}
		if (offset < 512 * 1024 - 4096) {
			return this.addr_256k + offset;
		}
		if (offset < 1024 * 1024 - 4096) {
			return this.addr_512k + offset;
		}
		if (offset < 2 * 1024 * 1024 - 4096) {
			return this.addr_1m + offset;
		}
		if (offset < 4 * 1024 * 1024 - 4096) {
			return this.addr_2m + offset;
		}
		if (offset < 8 * 1024 * 1024 - 4096) {
			return this.addr_4m + offset;
		}
		if (offset < 16 * 1024 * 1024 - 4096) {
			return this.addr_8m + offset;
		}
		if (offset < 32 * 1024 * 1024 - 4096) {
			return this.addr_16m + offset;
		}
		if (offset < 64 * 1024 * 1024 - 4096) {
			return this.addr_32m + offset;
		}
		if (offset < 128 * 1024 * 1024 - 4096) {
			return this.addr_64m + offset;
		}
		if (offset < 256 * 1024 * 1024 - 4096) {
			return this.addr_128m + offset;
		}
		if (offset < 512 * 1024 * 1024 - 4096) {
			return this.addr_256m + offset;
		}
		if (offset < 1024 * 1024 * 1024 - 4096) {
			return this.addr_512m + offset;
		}
		throw new IllegalArgumentException();
	}
	
	public final void free() {
		this.position.set(0);
		this.capacity = 0;
		this.addr_4k = 0;
		this.addr_8k = 0;
		this.addr_16k = 0;
		this.addr_32k = 0;
		this.addr_64k = 0;
		this.addr_128k = 0;
		this.addr_256k = 0;
		this.addr_512k = 0;
		this.addr_1m = 0;
		this.addr_2m = 0;
		this.addr_4m = 0;
		this.addr_8m = 0;
		this.addr_16m = 0;
		this.addr_32m = 0;
		this.addr_64m = 0;
		for (ByteBuffer i:this.allocated) {
			MemoryManager.free(i);
		}
		this.allocated.clear();
	}

	public int position() {
		return this.position.get();
	}

	public byte getByte(int offset) {
		byte value = Unsafe.getByte(getAddress(offset));
		return value;
	}

	public void putByte(int offset, byte value) {
		Unsafe.putByte(getAddress(offset), value);
	}

	public final int getInt(int offset) {
		int value = Unsafe.getInt(getAddress(offset));
		return value;
	}

	public final int getIntVolatile(int offset) {
		int value = Unsafe.getIntVolatile(getAddress(offset));
		return value;
	}

	public void putInt(int offset, int value) {
		Unsafe.putInt(getAddress(offset), value);
	}

	public final void putIntVolatile(int offset, int value) {
		Unsafe.putIntVolatile(getAddress(offset), value);
	}

	public final boolean compareAndSwapInt(int offset, int expected, int value) {
		return Unsafe.compareAndSwapInt(getAddress(offset), expected, value);
	}

	public final long getLong(int offset) {
		long value = Unsafe.getLong(getAddress(offset));
		return value;
	}
	
	public final long getLongVolatile(int offset) {
		long value = Unsafe.getLongVolatile(getAddress(offset));
		return value;
	}
	
	public final void putLong(int offset, long value) {
		Unsafe.putLong(getAddress(offset), value);
	}
	
	public final void putLongVolatile(int offset, long value) {
		Unsafe.putLongVolatile(getAddress(offset), value);
	}

	public void save(File file) throws IOException {
		try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
			int bytesLeft = this.position();
			for (ByteBuffer i:this.allocated) {
				ByteBuffer stuff = i.duplicate();
				int bytesToWrite = Math.min(i.capacity(), bytesLeft);
				stuff.position(0);
				stuff.limit(bytesToWrite);
				if (bytesToWrite != raf.getChannel().write(stuff)) {
					throw new IOException("bytes written is incorrect");
				}
				bytesLeft -= bytesToWrite;
			}
		}
	}

	public void load(File file) throws IOException {
		if (this.position.get() != 0) {
			throw new IllegalArgumentException();
		}
		if (this.allocated.size() != 1) {
			throw new IllegalArgumentException();
		}
		try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
			long length = file.length();
			if (length > DEFAULT_CAPACITY) {
				throw new IllegalArgumentException("file length is bigger than buffer capacity: " + length);
			}
			int bytesLeft = (int)length;
			while (bytesLeft > 0) {
				if (getRemaining() <= 0) {
					grow(this.capacity);
					continue;
				}
				int bytesToRead = Math.min(bytesLeft, getRemaining());
				ByteBuffer buf = this.allocated.get(this.allocated.size()-1);
				buf.position(0);
				if (bytesToRead != raf.getChannel().read(buf)) {
					throw new IOException("incorrect bytes read: " + file);
				}
				this.position.set(position() + bytesToRead);
				bytesLeft -= bytesToRead;
			}
		}
	}

	public final int getRemaining() {
		return this.capacity - position();
	}
}
