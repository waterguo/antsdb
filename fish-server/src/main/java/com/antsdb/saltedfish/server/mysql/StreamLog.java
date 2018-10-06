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
package com.antsdb.saltedfish.server.mysql;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;
import com.google.common.base.Charsets;

import io.netty.buffer.ByteBuf;

/**
 * 
 * @author *-xguo0<@
 */
public class StreamLog implements Closeable {
    static Logger _log = UberUtil.getThisLogger();
    
    private static final short MAGIC = 0x7777;

    File file;
    RandomAccessFile raf;
    FileChannel ch;
    String mode;
    
    public StreamLog(File file, String mode) {
        this.file = file;
        this.mode = mode;
    }
    
    public void open() throws IOException {
        this.raf = new RandomAccessFile(file, mode);
        if (mode.equals("rw")) {
            this.raf.setLength(0);
            this.raf.seek(0);
        }
    }
    
    @Override
    public void close() {
        if (this.raf != null) {
            IOUtils.closeQuietly(this.raf);
            this.raf = null;
        }
    }
    
    synchronized public void log(int streamId, ByteBuf data) throws IOException {
        ByteBuffer buf = data.nioBuffer();
        this.raf.writeShort(MAGIC);
        this.raf.writeInt(streamId);
        this.raf.writeInt(buf.remaining());
        this.raf.getChannel().write(buf);
    }
    
    synchronized public void log(int streamId, ByteBuffer buf) throws IOException {
        this.raf.writeShort(MAGIC);
        this.raf.writeInt(streamId);
        this.raf.writeInt(buf.remaining());
        this.raf.getChannel().write(buf);
    }
    
    synchronized public void log(int streamId, String msg) throws IOException {
        this.raf.writeShort(MAGIC);
        this.raf.writeInt(streamId);
        byte[] bytes = msg.getBytes(Charsets.UTF_8);
        this.raf.writeInt(bytes.length);
        this.raf.write(bytes);
    }
    
    public void replay(int offset, StreamLogReplayHandler handler) throws IOException {
        if (!mode.equals("r")) {
            throw new IllegalArgumentException();
        }
        MappedByteBuffer map = this.raf.getChannel().map(MapMode.READ_ONLY, 0, raf.length());
        map.position(offset);
        while (map.hasRemaining()) {
            int magic = map.getShort();
            if (magic != MAGIC) {
                throw new IllegalArgumentException("magic missing");
            }
            int streamId = map.getInt();
            int length = map.getInt();
            ByteBuffer data = map.slice();
            data.limit(length);
            handler.data(this.file, map.position(), streamId, data);
            map.position(map.position() + length);
        }
    }
}
