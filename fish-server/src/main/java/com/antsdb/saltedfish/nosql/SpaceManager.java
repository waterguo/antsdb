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
package com.antsdb.saltedfish.nosql;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * manages storage space
 *  
 * @author wgu0
 */
public final class SpaceManager {
	final static int SIG = 0x73746e61;
	final static byte VERSION = 0;
	final static int DEFAULT_FILE_SIZE = 1024*1024*64; 
	final static int HEADER_SIZE = 1024;
	final static int OFFSET_SIG = 0;
	final static int OFFSET_VERSION = 4;
	
	static Logger _log = UberUtil.getThisLogger();
	
	File home;
	boolean mutable;
	volatile long[] addresses = new long[100];
	volatile Space[] spaces = new Space[100];
	volatile int top = -1;
	private int fileSize;

	public SpaceManager(File home) {
		this(home, true, DEFAULT_FILE_SIZE);
	}
	
	public SpaceManager(File home, int filesize) {
		this(home, true, filesize);
	}
	
	public SpaceManager(File home, boolean mutable) {
		this(home, mutable, DEFAULT_FILE_SIZE);
	}

	public SpaceManager(File home, boolean mutable, int filesize) {
		this.home = home;
		this.mutable = mutable;
		this.fileSize = filesize;
	}
	
	public void init() throws IOException {
		// find all matched files
		
		List<File> files = new ArrayList<>();
		for (File file:home.listFiles()) {
	        Pattern ptn = Pattern.compile("^([0-9a-fA-F]{8})\\.dat$");
	        Matcher m = ptn.matcher(file.getName());
	        if (!m.find()) {
	            continue;
	        }
	        files.add(file);
		}

		// load them
		
		Collections.sort(files);
		for (File file:files) {
	        Pattern ptn = Pattern.compile("^([0-9a-fA-F]{8})\\.dat$");
	        Matcher m = ptn.matcher(file.getName());
	        m.find();
	        long idLong = Long.decode("0x" + m.group(1));
	        int id = (int)idLong;
	        Space space = new Space();
	        growArray(id);
	        this.spaces[id] = space;
	        space.file = file;
	        space.id = id;
	        this.top = Math.max(this.top, id);
		}
		
		// verify spaces
		
		for (int i=0; i<=this.top; i++) {
			Space space = this.spaces[i];
			if (space == null) {
				throw new HumpbackException("space " + i + " is missing");
			}
			MapMode mode = ((i == this.top) && mutable) ? MapMode.READ_WRITE : MapMode.READ_ONLY;
			open(space, mode);
		}
		
		// create first file if there is nothing
		
		if ((this.top == -1) && this.mutable) {
			grow(-1);
		}
	}
	
	@SuppressWarnings("resource")
	private void open(Space space, MapMode mode) throws IOException {
		String filemode = (mode == MapMode.READ_WRITE) ? "rw" : "r";
        RandomAccessFile raf = new RandomAccessFile(space.file, filemode);
        int fileLength = (int)raf.length();
        int size = (mode == MapMode.READ_WRITE) ? fileSize : fileLength;
    	space.raf = raf;
        space.channel = space.raf.getChannel();
        space.buf = space.channel.map(mode, 0, size);
        if (mode == MapMode.READ_WRITE) {
        	space.buf.load();
        }
        space.addr = UberUtil.getAddress(space.buf);
        space.spStart = makeSpacePointer(space.id, 0);
        space.spEnd = fileSize + space.spStart;
        _log.debug(String.format("mounted %s at 0x%016x", space.file.toString(), space.addr));
    	space.allocPointer = new AtomicInteger(fileLength);
        this.addresses[space.id] = space.addr;
	}

	public final long toMemory(long spRow) {
		int spaceId = (int)(spRow >> 32);
		int offset = (int)spRow & 0x7fffffff;
		Space space = this.spaces[spaceId];
		if ((offset < 0) || (offset >= space.buf.capacity())) {
			throw new IllegalArgumentException();
		}
		long base = addresses[spaceId];
		long address = base + offset;
		return address;
	}

	public String getLocation(long spRow) {
		int spaceId = (int)(spRow >> 32);
		int offset = (int)spRow & 0x7fffffff;
		Space space = this.spaces[spaceId];
		if ((offset < 0) || (offset >= space.buf.capacity())) {
			throw new IllegalArgumentException();
		}
		return String.format("%s@%x", space.file, offset);
	}

	public final long plus(long sp, int length, int bytes) {
		int spaceId = (int)(sp >> 32);
		int offset = (int)sp & 0x7fffffff;
		Space space = this.spaces[spaceId];
		if ((offset < 0) || (offset >= space.buf.capacity())) {
			throw new IllegalArgumentException();
		}
		offset += length;
		if ((offset + bytes) >= space.buf.capacity()) {
			spaceId++;
			if (spaceId > this.top) {
				return Long.MAX_VALUE;
			}
			offset = HEADER_SIZE;
			space = this.spaces[spaceId];
		}
		return makeSpacePointer(spaceId, offset);
	}
	
	public long nextSegment(long sp) {
		int spaceId = (int)(sp >> 32) + 1;
		if (spaceId >= this.spaces.length) {
			return -1;
		}
		Space space = this.spaces[spaceId];
		if (space == null) {
			return -1;
		}
		return makeSpacePointer(spaceId, HEADER_SIZE);
	}
	
	public final static long makeSpacePointer(long spaceId, long offset) {
		long sp = spaceId << 32 | offset;
		return sp;
	}

	public final static int getSpaceId(long sp) {
		long spaceId = sp >> 32;
		return (int)spaceId;
	}

	public ByteBuffer getBuffer(long spStart, long spEnd) {
		int spaceId = getSpaceId(spStart);
		if (spaceId != getSpaceId(spEnd)) {
			throw new IllegalArgumentException();
		}
		Space space = this.spaces[spaceId];
		ByteBuffer buf = space.buf.duplicate();
		buf.position((int)(spStart - space.spStart));
		buf.limit((int)(spEnd - space.spStart));
		return buf;
	}

	public final long alloc(int size) {
		for (;;) {
			int index = this.top;
			Space space = this.spaces[index];
			int p = space.allocPointer.get();
			if ((this.fileSize - p) < size) {
				grow(index);
				continue;
			}
			if (space.allocPointer.compareAndSet(p, p + size)) {
				long spMem = makeSpacePointer(space.id, p);
				return spMem;
			}
		}
	}
	
	private void growArray(int maxId) {
		while (this.spaces.length <= maxId) {
			long[] array = new long[this.addresses.length * 2];
			System.arraycopy(addresses, 0, array, 0, addresses.length);
			Space[] array2 = new Space[array.length];
			System.arraycopy(this.spaces, 0, array2, 0, addresses.length);
			this.addresses = array;
			this.spaces = array2;
		}
	}

	/**
	 * grow the space when it is full
	 */
	private void grow(int index) {
		synchronized(this) {
			if (!this.mutable) {
				throw new IllegalArgumentException();
			}
			
			if (index != this.top) {
				// lost race
				return;
			}
			Space space = new Space();
			space.id = this.top + 1;
			growArray(space.id);
			this.spaces[space.id] = space;
			String name = String.format("%08x.dat", space.id);
			space.file = new File(this.home, name);
			try {
				open(space, MapMode.READ_WRITE);
			}
			catch (IOException e) {
				throw new OutofSpaceException(e);
			}
			space.buf.order(ByteOrder.LITTLE_ENDIAN);
			space.buf.putInt(OFFSET_SIG, SIG);
			space.buf.put(OFFSET_VERSION, VERSION);
			space.allocPointer.set(HEADER_SIZE);
			this.top = space.id;
		}
	}

	public final long getAllocationPointer() {
		Space space = this.spaces[this.top];
		long spEnd = makeSpacePointer(space.id, space.allocPointer.get());
		return spEnd;
	}

	/**
	 * sync the mapped file up to the specified position
	 * 
	 * @param sp
	 */
	public final void force(long spStart) {
		force(spStart, getAllocationPointer());
	}
	
	/**
	 * sync the mapped file up to the specified position
	 * 
	 * @param sp
	 */
	public final void force(long spStart, long spEnd) {
		for (;;) {
			int spaceIdStart = getSpaceId(spStart);
			Space spaceStart = this.spaces[spaceIdStart];
			long offsetStart = spStart -spaceStart.spStart;
			int spaceIdEnd = getSpaceId(spEnd);
			long size;
			if (spaceIdEnd != spaceIdStart) {
				size = spaceStart.spEnd - spStart;
			}
			else {
				size = spEnd - spStart;
			}
			if (size != 0) {
				MappedByteBuffer buf;
				try {
					buf = spaceStart.channel.map(MapMode.READ_WRITE, offsetStart, size);
					buf.force();
					Unsafe.unmap(buf);
				}
				catch (IOException e) {
					throw new HumpbackException("unable to persist", e);
				}
				if (spaceIdEnd == spaceIdStart) {
					break;
				}
			}
			spStart = makeSpacePointer(spaceIdStart+1, 0);
		}
	}
	
	/**
	 * close everything
	 */
	public synchronized void close() {
		_log.info("closing space manager ...");
		for (int i=0; i<this.spaces.length; i++) {
			Space space = this.spaces[i];
			if (space == null) {
				continue;
			}
			this.spaces[i] = null;
			try {
				MappedByteBuffer b = space.buf;
				space.buf = null;
				Unsafe.unmap(b);
				if (space.allocPointer != null) {
					if (space.allocPointer.get() != space.raf.length()) {
						space.raf.setLength(space.allocPointer.get());
					}
				}
				space.raf.close();
				space.channel.close();
			}
			catch (IOException x) {
				// it shouldn't throw any exception
				_log.warn("errors when closing space manager", x);
			}
		}
	}

	public boolean isReadOnly() {
		return !this.mutable;
	}

	public long getStartSp() {
		for (Space i:this.spaces) {
			if (i != null) {
				return makeSpacePointer(i.id, HEADER_SIZE);
			}
		}
		return -1;
	}

	public long getSpaceStartSp(int spaceId) {
		return makeSpacePointer(spaceId, HEADER_SIZE);
	}
	
}
