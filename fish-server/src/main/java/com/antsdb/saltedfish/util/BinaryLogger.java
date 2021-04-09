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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import static java.nio.file.StandardOpenOption.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.Charsets;
import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.Unsafe;

/**
 * 
 * @author *-xguo0<@
 */
public final class BinaryLogger {
    private static Logger _log = UberUtil.getThisLogger();
    
    private File home;
    private int fileSize;
    private String prefix;
    private ThreadLocal<ByteBuffer> bufLocal;
    private volatile Unit unit;

    private static class Unit {
        MappedByteBuffer mmf;
        long pMMF;
        File file;
        AtomicInteger pos = new AtomicInteger();
    }
    
    public BinaryLogger(File home, String prefix, int fileSize, int entrySize) {
        if (fileSize <= entrySize) {
            throw new IllegalArgumentException();
        }
        this.home = home;
        this.prefix = prefix;
        this.fileSize = fileSize;
        this.bufLocal = ThreadLocal.withInitial(()->{ return ByteBuffer.allocateDirect(entrySize);});
    }
    
    public void log(String msg, Object... args) {
        try {
            ByteBuffer buf = this.bufLocal.get();
            buf.clear();
            buf.putShort((short)0x7777);
            buf.putInt(0);
            buf.putLong(UberTime.getTime());
            put(buf, msg);
            if (args != null) {
                for (Object i:args) {
                    putObject(buf, i);
                }
            }
            buf.putInt(2, buf.position());
            flush(buf);
        }
        catch (IOException x) {
            _log.warn("failed to log", x);
        }
    }

    public File getCurrentFile() {
        return (this.unit != null) ? this.unit.file : null;
    }
    
    private void flush(ByteBuffer buf) throws IOException {
        buf.flip();
        int size = buf.remaining();
        if (this.unit == null) {
            grow(this.unit);
        }
        for (;;) {
            Unit u = this.unit;
            int after = u.pos.addAndGet(size);
            if (after > this.fileSize) {
                grow(u);
                continue;
            }
            Unsafe.copyMemory(UberUtil.getAddress(buf), u.pMMF + after - size, size);
            break;
        }
    }

    private synchronized void grow(Unit u) throws IOException {
        if (this.unit != u) {
            // prevents duplication in concurrency
            return;
        }
        SimpleDateFormat format = new SimpleDateFormat("YYMMdd-HHmmss-SSS");
        String suffix = format.format(new Date());
        String filename = this.prefix + "." + suffix + ".blog";
        Unit nuevo = new Unit();
        nuevo.file = new File(this.home, filename);
        _log.debug("growing to {}", nuevo.file);
        try (FileChannel ch = FileChannel.open(nuevo.file.toPath(), CREATE, READ, WRITE)) {
            nuevo.mmf = ch.map(MapMode.READ_WRITE, 0, this.fileSize);
            nuevo.pMMF = UberUtil.getAddress(nuevo.mmf);
        }
        this.unit = nuevo;
    }

    private boolean ensure(ByteBuffer buf, int size) {
        return buf.remaining() >= size;
    }

    private boolean writeNull(ByteBuffer buf) {
        if (!ensure(buf, 1)) {
            return false;
        }
        buf.put(TypeId.NULL);
        return true;
    }

    private boolean put(ByteBuffer buf, byte value) {
        if (!ensure(buf, 2)) {
            return false;
        }
        buf.put(TypeId.BYTE);
        buf.put(value);
        return true;
    }
    
    private boolean put(ByteBuffer buf, short value) {
        if (!ensure(buf, 3)) {
            return false;
        }
        buf.put(TypeId.SHORT);
        buf.putShort(value);
        return true;
    }

    private boolean put(ByteBuffer buf, int value) {
        if (!ensure(buf, 5)) {
            return false;
        }
        buf.put(TypeId.INT);
        buf.putInt(value);
        return true;
    }

    private boolean put(ByteBuffer buf, long value) {
        if (!ensure(buf, 9)) {
            return false;
        }
        buf.put(TypeId.LONG);
        buf.putLong(value);
        return true;
    }

    private boolean put(ByteBuffer buf, float value) {
        if (!ensure(buf, 5)) {
            return false;
        }
        buf.put(TypeId.FLOAT);
        buf.putFloat(value);
        return true;
    }

    private boolean put(ByteBuffer buf, double value) {
        if (!ensure(buf, 9)) {
            return false;
        }
        buf.put(TypeId.DOUBLE);
        buf.putDouble(value);
        return true;
    }

    private void putVariableLength(ByteBuffer buf, byte type, ByteBuffer bytes) {
        buf.put(type);
        int left = buf.remaining() - 2;
        bytes.mark();
        if (bytes.remaining() <= left) {
            buf.putShort((short)bytes.remaining());
            buf.put(bytes);
        }
        else {
            buf.putShort((short)left);
            bytes.limit(bytes.position() + left);
            buf.put(bytes);
        }
        bytes.reset();
    }
    
    private boolean put(ByteBuffer buf, String value) {
        if (value == null) {
            return writeNull(buf);
        }
        if (!ensure(buf, 4)) {
            return false;
        }
        ByteBuffer bytes = Charsets.UTF_8.encode(value);
        putVariableLength(buf, TypeId.STRING, bytes);
        return true;
    }

    private boolean put(ByteBuffer buf, char[] value) {
        if (value == null) {
            return writeNull(buf);
        }
        if (!ensure(buf, 4)) {
            return false;
        }
        ByteBuffer bytes = Charsets.UTF_8.encode(CharBuffer.wrap(value));
        putVariableLength(buf, TypeId.STRING, bytes);
        return true;
    }

    private boolean put(ByteBuffer buf, CharBuffer value) {
        if (value == null) {
            return writeNull(buf);
        }
        if (!ensure(buf, 4)) {
            return false;
        }
        ByteBuffer bytes = Charsets.UTF_8.encode(value);
        putVariableLength(buf, TypeId.STRING, bytes);
        return true;
    }

    private boolean put(ByteBuffer buf, byte[] value) {
        if (value == null) {
            return writeNull(buf);
        }
        if (!ensure(buf, 4)) {
            return false;
        }
        putVariableLength(buf, TypeId.BINARY, ByteBuffer.wrap(value));
        return true;
    }

    private boolean put(ByteBuffer buf, ByteBuffer value) {
        if (value == null) {
            return writeNull(buf);
        }
        if (!ensure(buf, 4)) {
            return false;
        }
        putVariableLength(buf, TypeId.BINARY, value);
        return true;
    }
    
    private boolean putUnknown(ByteBuffer buf, Object value) {
        if (value == null) {
            return writeNull(buf);
        }
        if (!ensure(buf, 5)) {
            return false;
        }
        String msg = value.getClass().getName() + ":" + value;
        ByteBuffer bytes = Charsets.UTF_8.encode(msg);
        putVariableLength(buf, TypeId.UNKNOWN, bytes);
        return true;
    }
    
    private int putObject(ByteBuffer buf, Object value) {
        int result = 1;
        if (value == null) {
            writeNull(buf);
        }
        else if (value instanceof Byte) {
            put(buf, (Byte)value);
        }
        else if (value instanceof Short) {
            put(buf, (Short)value);
        }
        else if (value instanceof Integer) {
            put(buf, (Integer)value);
        }
        else if (value instanceof Long) {
            put(buf, (Long)value);
        }
        else if (value instanceof Float) {
            put(buf, (Float)value);
        }
        else if (value instanceof Double) {
            put(buf, (Double)value);
        }
        else if (value instanceof String) {
            put(buf, (String)value);
        }
        else if (value instanceof byte[]) {
            put(buf, (byte[])value);
        }
        else if (value instanceof ByteBuffer) {
            put(buf, (ByteBuffer)value);
        }
        else if (value instanceof char[]) {
            put(buf, (char[])value);
        }
        else if (value instanceof CharBuffer) {
            put(buf, (CharBuffer)value);
        }
        else if (value instanceof Object[]) {
            result = 0;
            for (Object i:(Object[])value) {
                result += putObject(buf, i);
            }
        }
        else {
            putUnknown(buf, value);
        }
        return result;
    }
}
