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
package com.antsdb.saltedfish.server.mysql;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 
 * @author *-xguo0<@
 */
public abstract class ChannelWriter {
    ByteBuffer buf;
    
    protected abstract void writeDirect(byte[] bytes) throws IOException;
    protected abstract void writeDirect(ByteBuffer bytes) throws IOException;
    
    ChannelWriter() {
        this(16 * 1024);
    }
    
    ChannelWriter(int cacheSize) {
        this.buf = ByteBuffer.allocateDirect(cacheSize);
    }
    
    public void write(byte[] bytes) {
        try {
            if (bytes.length > this.buf.remaining()) {
                flush();
            }
            if (bytes.length < this.buf.remaining()) {
                this.buf.put(bytes);
            }
            else {
                writeDirect(bytes);
            }
        }
        catch (IOException x) {
            throw new RuntimeException(x);
        }
    }
    
    public void write(ByteBuffer bytes) {
        try {
            if (bytes.remaining() > this.buf.remaining()) {
                flush();
            }
            if (bytes.remaining() < this.buf.remaining()) {
                this.buf.put(bytes);
            }
            else {
                writeDirect(bytes);
            }
        }
        catch (IOException x) {
            throw new RuntimeException(x);
        }
    }
    
    public void flush() {
        try {
            if (this.buf.position() > 0) {
                this.buf.flip();
                writeDirect(this.buf);
            }
            this.buf.clear();
        }
        catch (IOException x) {
            throw new RuntimeException(x);
        }
    }
}
