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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

/**
 * debug facility for unsafe 
 *  
 * @author wgu0
 */
public final class UnsafeDebug {
	final static File FILE = new File("debug");
	static Logger _log = UberUtil.getThisLogger();
	static MappedByteBuffer _buf;
	
	static {
		try {
			FILE.delete();
			_log.info("unsafe debug: {}", FILE.getAbsolutePath());
			RandomAccessFile raf = new RandomAccessFile(FILE, "rw");
			_buf = raf.getChannel().map(MapMode.READ_WRITE, 0, 1024 * 1024);
			raf.close();
		}
		catch (Exception x) {
			_log.error("unsafe debug failed", x);
			throw new Error(x);
		}
	}
	
	public static ByteBuffer getBuffer() {
		return _buf;
	}

	public static void putLong(int pos, long value) {
		_buf.putLong(pos, value);
	}

	public static void putBoolean(int pos, boolean value) {
		_buf.put(pos, (byte)(value ? 1 : 0));
	}
}
