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

import java.nio.ByteBuffer;

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
        this.buf.flip();
        ByteBuffer newone = ByteBuffer.allocate((this.buf.remaining() + size) * 3 / 2);
        newone.put(this.buf);
        this.buf = newone;
    }
    
    public ByteBuffer getWrapped() {
        return this.buf;
    }
}
