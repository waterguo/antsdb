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
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author wgu0
 */
public class MemoryMappedFile {
	static Logger _log = UberUtil.getThisLogger();
	
	File file;
	int size;
	long addr;
	MappedByteBuffer buf;
	
	public MemoryMappedFile(File file, String mode) throws IOException {
		this(file, file.length(), mode);
	}
	
	public MemoryMappedFile(File file, long size, String mode) throws IOException {
		if (size >= Integer.MAX_VALUE) {
			throw new IllegalArgumentException("jvm doesn't support mapped file more than 2g");
		}
		MapMode mapmode = null;
		if (mode.equals("r")) {
			mapmode = MapMode.READ_ONLY;
		}
		else if (mode.equals("rw")) {
			mapmode = MapMode.READ_WRITE;
		}
		else {
			throw new IllegalArgumentException();
		}
		this.file = file;
		try (RandomAccessFile raf = new RandomAccessFile(file, mode)) {
			FileChannel channel = raf.getChannel();
			this.buf = channel.map(mapmode, 0, size);
			this.buf.order(ByteOrder.nativeOrder());
			this.addr = UberUtil.getAddress(buf);
		}
        _log.debug(String.format("mounted %s at 0x%016x", file.toString(), addr));
	}
	
	public void close() {
		unmap();
	}
	
	public long getAddress() {
		return this.addr;
	}
	
	public int getSize() {
		return this.size;
	}

	public void force() {
		this.buf.force();
	}

	public void unmap() {
        _log.debug(String.format("%s 0x%016x is unmounted", file.toString(), addr));
		Unsafe.unmap(this.buf);
		this.buf = null;
		this.addr = 0;
	}
	
	public boolean isReadOnly() {
		return this.buf.isReadOnly();
	}
}
