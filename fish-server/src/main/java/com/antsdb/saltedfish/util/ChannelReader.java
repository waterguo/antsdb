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
package com.antsdb.saltedfish.util;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public class ChannelReader implements Closeable {
    ReadableByteChannel ch;
    ByteBuffer buf = ByteBuffer.allocate(8);
    boolean isEof = false;

    public ChannelReader(ReadableByteChannel ch) {
        super();
        this.ch = ch;
    }
    
    public byte readByte() throws IOException {
        this.buf.position(0);
        this.buf.limit(1);
        readFully(buf);
        buf.flip();
        return buf.get();
    }
    
    public int readInt() throws IOException {
        this.buf.position(0);
        this.buf.limit(4);
        readFully(buf);
        buf.flip();
        return buf.getInt();
    }

    public long readLong() throws IOException {
        this.buf.position(0);
        this.buf.limit(8);
        readFully(buf);
        buf.flip();
        return buf.getLong();
    }

    /**
     * read until it reaches the limit of the buffer
     * 
     * @param buf
     * @throws IOException 
     */
    public void readFully(ByteBuffer buf) throws IOException {
        if (buf.remaining() < 0)
            throw new IndexOutOfBoundsException();
        while (buf.remaining() > 0) {
            int count = ch.read(buf);
            if (count < 0)
                throw new EOFException();
        }
    }

    @Override
    public void close() throws IOException {
        this.ch.close();
    }
}
