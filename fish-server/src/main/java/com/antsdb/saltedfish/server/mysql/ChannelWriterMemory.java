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

import java.nio.ByteBuffer;

import com.antsdb.saltedfish.cpp.AllocPoint;
import com.antsdb.saltedfish.cpp.MemoryManager;

/**
 * 
 * @author *-xguo0<@
 */
public class ChannelWriterMemory extends ChannelWriter {

    ChannelWriterMemory() {
        super(1024);
    }
    
    @Override
    protected void writeDirect(byte[] bytes) {
        grow(bytes.length);
        this.buf.put(bytes);
    }

    @Override
    protected void writeDirect(ByteBuffer bytes) {
        grow(bytes.remaining());
        this.buf.put(bytes);
    }

    @Override
    public void write(byte[] bytes) {
        writeDirect(bytes);
    }

    @Override
    public void write(ByteBuffer bytes) {
        writeDirect(bytes);
    }

    private void grow(int size) {
        if (this.buf.remaining() > size) {
            return;
        }
        int newBufferSize = (this.buf.capacity() + size) * 3 / 2; 
        this.buf = MemoryManager.growImmortal(AllocPoint.CHANNEL_WRITER, this.buf, newBufferSize);
    }
    
    public ByteBuffer getWrapped() {
        return this.buf;
    }
}
