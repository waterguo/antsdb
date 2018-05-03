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

import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
final class BufferWriterNio extends BufferWriter {
    ByteBuffer buf;
    long addr;

    BufferWriterNio() {
        this.buf = ByteBuffer.allocateDirect(4096);
        this.addr = UberUtil.getAddress(this.buf);
    }
    
    @Override
    public Object getWrapped() {
        return this.buf;
    }

    @Override
    public void writeBytes(byte[] bytes) {
        check(bytes.length);
        this.buf.put(bytes);
    }

    @Override
    public void writeBytes(ByteBuffer bytes) {
        check(bytes.remaining());
        this.buf.put(bytes);
    }
    
    @Override
    public void readBytes(int start, byte[] bytes) {
        Unsafe.getBytes(this.addr, bytes);
    }
    
    @Override
    public void writeLong(long value) {
        check(8);
        this.buf.putLong(value);
    }

    @Override
    public void writeByte(byte value) {
        check(1);
        this.buf.put(value);
    }

    @Override
    public int position() {
        return this.buf.position();
    }

    @Override
    public void position(int pos) {
        this.buf.position(pos);
    }

    @Override
    public void writeShort(short value) {
        check(2);
        this.buf.putShort(value);
    }

    @Override
    public void writeInt(int value) {
        check(4);
        this.buf.putInt(0);
    }

    private void check(int nbytes) {
        if (this.buf.remaining() > nbytes) {
            return;
        }
        int newsize = (this.buf.position() + nbytes) * 3 / 2;
        ByteBuffer newone = ByteBuffer.allocateDirect(newsize);
        this.buf.flip();
        newone.put(this.buf);
        this.buf = newone;
    }

    @Override
    public void writeBytes(long pValue, int len) {
        check(len);
        int pos = position();
        Unsafe.copyMemory(pValue, this.addr + pos, len);
        position(pos + len);
    }
}
