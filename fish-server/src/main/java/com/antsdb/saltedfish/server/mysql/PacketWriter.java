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
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.Calendar;

import com.antsdb.saltedfish.cpp.AllocPoint;
import com.antsdb.saltedfish.cpp.FishUtf8;
import com.antsdb.saltedfish.cpp.MemoryManager;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public final class PacketWriter {
    private ByteBuffer buf;
    long addr;

    public PacketWriter() {
        this.buf = MemoryManager.allocImmortal(AllocPoint.PACKET_WRITER, 4096);
        this.addr = UberUtil.getAddress(this.buf);
    }

    public Object getWrapped() {
        return this.buf;
    }

    public void writeBytes(byte[] bytes) {
        check(bytes.length);
        this.buf.put(bytes);
    }

    public void writeBytes(ByteBuffer bytes) {
        check(bytes.remaining());
        this.buf.put(bytes);
    }
    
    public void readBytes(int start, byte[] bytes) {
        Unsafe.getBytes(this.addr, bytes);
    }
    
    public void writeByte(byte value) {
        check(1);
        this.buf.put(value);
    }

    public int position() {
        return this.buf.position();
    }

    public void position(int pos) {
        this.buf.position(pos);
    }

    public void writeShort(short value) {
        check(2);
        this.buf.putShort(value);
    }

    public void writeInt(int value) {
        writeByte((byte) (value & 0xff));
        writeByte((byte) (value >>> 8));
    }

    public void writeLong(long value) {
        writeByte((byte) (value & 0xff));
        writeByte((byte) (value >>> 8));
        writeByte((byte) (value >>> 16));
        writeByte((byte) (value >>> 24));
    }
    
    private void check(int nbytes) {
        if (this.buf.remaining() > nbytes) {
            return;
        }
        int newsize = (this.buf.position() + nbytes) * 3 / 2;
        this.buf = MemoryManager.growImmortal(AllocPoint.PACKET_WRITER, this.buf, newsize);
    }

    public void writeBytes(long pValue, int len) {
        check(len);
        int pos = position();
        Unsafe.copyMemory(pValue, this.addr + pos, len);
        position(pos + len);
    }
    
    public void writeUB2(int value) {
        writeByte((byte) (value & 0xff));
        writeByte((byte) (value >>> 8));
    }

    public void writeLongInt(int value) {
        writeByte((byte) (value & 0xff));
        writeByte((byte) (value >>> 8));
        writeByte((byte) (value >>> 16));
    }

    public void writeString(String value) {
        writeStringNoNull(value);
        writeByte((byte)0);
    }
    
    public void writeStringNoNull(String value) {
        Charset cs = Charset.defaultCharset();
        writeBytes(cs.encode(value));
    }
    
    public void writeUB3(int value) {
        writeByte((byte) (value & 0xff));
        writeByte((byte) (value >>> 8));
        writeByte((byte) (value >>> 16));
    }

    public void writeUB4(long value) {
        writeByte((byte) (value & 0xff));
        writeByte((byte) (value >>> 8));
        writeByte((byte) (value >>> 16));
        writeByte((byte) (value >>> 24));
    }

    public void writeWithLength(byte[] src) {
        if (src==null || src.length==0) {
            writeByte((byte)0);
        }
        else {
            int length = src==null? 0: src.length;
            writeLength(length);
            writeBytes(src);
        }
    }

    public void writeLength(long value) {
        if (value < 251) {
            writeByte((byte) value);
        } else if (value < 0x10000L) {
            writeByte((byte) 252);
            writeUB2((int) value);
        } else if (value < 0x1000000L) {
            writeByte((byte) 253);
            writeUB3((int) value);
        } else {
            writeByte((byte) 254);
            writeUB4(value);
        }
    }

    public void writeLenString(String s, Charset encoder) {
        if (s == null) {
            writeLength(0);
            return;
        }
        ByteBuffer bb = encoder.encode(s);
        writeLength(bb.remaining());
        writeBytes(bb);
    }

    public void writeLongLong(long value) {
        writeByte((byte) (value & 0xff));
        writeByte((byte) (value >>> 8));
        writeByte((byte) (value >>> 16));
        writeByte((byte) (value >>> 24));
        writeByte((byte) (value >>> 32));
        writeByte((byte) (value >>> 40));
        writeByte((byte) (value >>> 48));
        writeByte((byte) (value >>> 56));
    }

    public void writeDate(java.util.Date date) {
        writeByte((byte)7);
        if (date.getTime() == Long.MIN_VALUE) {
            // 0 date in mysql
            writeUB2(0);
            writeByte((byte)0);
            writeByte((byte)0);
            writeByte((byte)0);
            writeByte((byte)0);
            writeByte((byte)0);
        }
        else {
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            writeUB2(cal.get(Calendar.YEAR));
            writeByte((byte)(cal.get(Calendar.MONTH)+1));
            writeByte((byte)cal.get(Calendar.DAY_OF_MONTH));
            writeByte((byte)cal.get(Calendar.HOUR_OF_DAY));
            writeByte((byte)cal.get(Calendar.MINUTE));
            writeByte((byte)cal.get(Calendar.SECOND));
        }
    }

    public void writeTimestamp(Timestamp date) {
        writeByte((byte)11);
        if (date.getTime() == Long.MIN_VALUE) {
            // 0 datetime in mysql
            writeUB2(0);
            writeInt(0);
            writeByte((byte)0);
            writeUB4(0);
        }
        else {
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            writeUB2(cal.get(Calendar.YEAR));
            writeByte((byte)(cal.get(Calendar.MONTH)+1));
            writeByte((byte)cal.get(Calendar.DAY_OF_MONTH));
            writeByte((byte)cal.get(Calendar.HOUR_OF_DAY));
            writeByte((byte)cal.get(Calendar.MINUTE));
            writeByte((byte)cal.get(Calendar.SECOND));
            writeUB4(cal.get(Calendar.MILLISECOND)*1000);
        }
    }

    public void writeLenStringUtf8(long pValue) {
        if (pValue == 0) {
            writeLength(0);
            return;
        }
        int len = FishUtf8.getSize(Value.FORMAT_UTF8, pValue) - FishUtf8.HEADER_SIZE;
        writeLength(len);
        long pData = pValue + FishUtf8.HEADER_SIZE;
        writeBytes(pData, len);
    }

    public void clear() {
        this.buf.clear();
    }

    public void flush(ChannelWriter out) {
        this.buf.flip();
        out.write(this.buf);
    }

    public void close() {
        MemoryManager.freeImmortal(AllocPoint.PACKET_WRITER, this.buf);
        this.buf = null;
    }
}
