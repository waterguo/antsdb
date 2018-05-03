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

import io.netty.buffer.ByteBuf;

/**
 * 
 * @author *-xguo0<@
 */
final class BufferWriterNetty extends BufferWriter {
    ByteBuf buf;

    public BufferWriterNetty(ByteBuf buffer) {
        this.buf = buffer;
    }

    @Override
    public Object getWrapped() {
        return this.buf;
    }

    @Override
    public void writeBytes(byte[] bytes) {
        this.buf.writeBytes(bytes);
    }

    @Override
    public void writeBytes(ByteBuffer bytes) {
        this.buf.writeBytes(bytes);
    }
    
    @Override
    public void readBytes(int pos, byte[] bytes) {
        if (pos + bytes.length > this.buf.capacity()) {
            throw new IndexOutOfBoundsException();
        }
        this.buf.getBytes(pos, bytes);
    }

    @Override
    public void writeLong(long value) {
        this.buf.writeLong(value);
    }

    @Override
    public void writeByte(byte value) {
        this.buf.writeByte(value);
    }

    @Override
    public int position() {
        return this.buf.writerIndex();
    }

    @Override
    public void position(int pos) {
        this.buf.writerIndex(pos);
    }

    @Override
    public void writeShort(short value) {
        this.buf.writeShort(value);
    }

    @Override
    public void writeInt(int value) {
        this.buf.writeInt(value);
    }

    @Override
    public void writeBytes(long pValue, int len) {
        if (this.buf.hasMemoryAddress()) {
            this.buf.ensureWritable(len, true);
            long addr = this.buf.memoryAddress();
            int pos = this.buf.writerIndex();
            Unsafe.copyMemory(pValue, addr + pos, len);
            this.buf.writerIndex(pos + len);
        }
        else {
            for (int i=0; i<len; i++) {
                byte bt = Unsafe.getByte(pValue + i);
                this.buf.writeByte(bt);
            }
        }
    }
}
