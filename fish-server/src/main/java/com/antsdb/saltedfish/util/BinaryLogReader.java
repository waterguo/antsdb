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

import static java.nio.file.StandardOpenOption.READ;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.Charsets;

import com.antsdb.saltedfish.cpp.Unsafe;

/**
 * 
 * @author *-xguo0<@
 */
public final class BinaryLogReader implements Closeable {
    private File file;
    private MappedByteBuffer mmf;
    private ByteBuffer entry;
    private int offset;

    public BinaryLogReader(File file) {
        this.file = file;
    }
    
    public void open() throws IOException {
        try (FileChannel ch = FileChannel.open(this.file.toPath(), READ)) {
            this.mmf = ch.map(MapMode.READ_ONLY, 0, this.file.length());
        }
    }
    
    public boolean next() {
        this.offset = mmf.position();
        if (mmf.limit() <= 6) {
            return false;
        }
        if (mmf.getShort() != 0x7777) {
            return false;
        }
        int length = mmf.getInt();
        this.entry = mmf.slice();
        this.entry.limit(length - 6);
        this.mmf.position(this.mmf.position() + length - 6);
        return true;
    }
    
    public long getTime() {
        long result = this.entry.getLong(0);
        return result;
    }
    
    private String readString() {
        int length = this.entry.getShort();
        ByteBuffer bytes = this.entry.slice();
        bytes.limit(length);
        String result = Charsets.UTF_8.decode(bytes).toString();
        this.entry.position(this.entry.position() + length);
        return result;
    }
    
    private byte[] readVariableLength() {
        int length = this.entry.getShort();
        byte[] bytes = new byte[length];
        this.entry.get(bytes);
        return bytes;
    }
    
    public String getMessage() {
        this.entry.position(9);
        String result = readString();
        return result;
    }
    
    public List<Object> getArgs() {
        int length = this.entry.getShort(9);
        this.entry.position(11 + length);
        List<Object> result = new ArrayList<>();
        while (this.entry.hasRemaining()) {
            result.add(readValue());
        }
        return result;
    }

    private Object readValue() {
        Object result;
        byte type = this.entry.get();
        switch (type) {
        case TypeId.NULL:
            result = null;
            break;
        case TypeId.BYTE:
            result = this.entry.get();
            break;
        case TypeId.SHORT:
            result = this.entry.getShort();
            break;
        case TypeId.INT:
            result = this.entry.getInt();
            break;
        case TypeId.FLOAT:
            result = this.entry.getFloat();
            break;
        case TypeId.LONG:
            result = this.entry.getLong();
            break;
        case TypeId.DOUBLE:
            result = this.entry.getDouble();
            break;
        case TypeId.TIMESTAMP:
            result = new Timestamp(this.entry.getLong());
            break;
        case TypeId.STRING:
            result = readString();
            break;
        case TypeId.BINARY:
            result = readVariableLength();
            break;
        case TypeId.UNKNOWN:
            result = readString();
            break;
        default:
            throw new IllegalArgumentException("type not supported: " + type);    
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        if (this.mmf != null) {
            Unsafe.unmap(this.mmf);
            this.mmf = null;
        }
        this.entry = null;
    }

    public int getOffset() {
        return this.offset;
    }
}
