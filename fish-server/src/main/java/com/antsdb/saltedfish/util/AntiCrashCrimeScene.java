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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;

import com.antsdb.saltedfish.charset.Utf8;
import com.antsdb.saltedfish.cpp.Unsafe;

/**
 * crime scene logging that can survive jvm crash 
 *  
 * @author *-xguo0<@
 */
public final class AntiCrashCrimeScene {
    static AntiCrashCrimeScene _singlton;
    
    private File file;
    private List<MappedByteBuffer> mmfs = new ArrayList<>();
    private List<Unit> units = new ArrayList<>();
    private RandomAccessFile raf;
    private FileChannel ch;
    private ThreadLocal<Unit> addr = ThreadLocal.withInitial(()->{return alloc();});
    
    public static class Unit {
        static final int SIZE = 4 * 1024;
        static final int OFFSET_THREAD_ID=0;
        static final int OFFSET_TIMESTAMP=8;
        static final int OFFSET_VALUES=0x10;
        
        ByteBuffer buf; 
        long addr;
        Thread owner;
        
        void setThreadId() {
            Unsafe.putLong(this.addr + OFFSET_THREAD_ID, Thread.currentThread().getId());
        }
        
        public long getThreadId() {
            return Unsafe.getLong(this.addr + OFFSET_THREAD_ID);
        }
        
        void setTimestamp() {
            Unsafe.putLong(this.addr + OFFSET_TIMESTAMP, UberTime.getTime());
        }
        
        public long getTimestamp() {
            return Unsafe.getLong(this.addr + OFFSET_TIMESTAMP);
        }
        
        public Object[] getValues() {
            this.buf.position(OFFSET_VALUES);
            return AntiCrashCrimeScene.getValues(this.buf);
        }
    }

    public AntiCrashCrimeScene(File file) {
        this.file = file;
    }
    
    public static File getSceneFile() {
        return (_singlton != null) ? _singlton.file : null;
    }
    
    public static void init(File file) throws IOException {
        if (_singlton != null) {
            return;
        }
        _singlton = new AntiCrashCrimeScene(file);
        _singlton.open(true);
        file.deleteOnExit();
    }
    
    public void open() throws IOException {
        open(false);
    }
    
    private void open(boolean write) throws IOException {
        if (this.raf != null) {
            return;
        }
        
        // rename the existing file so we dont accidently lose previous crash scene
        if (write && file.exists()) {
            String filename = file.getName();
            filename = System.currentTimeMillis() + "-" + filename;
            File backup = new File(file.getParentFile(), filename);
            file.renameTo(backup);
        }
        
        // open it
        this.raf = new RandomAccessFile(file, write ? "rw" : "r");
        this.ch = this.raf.getChannel();
        
        // map units if this is read only
        if (!write) {
            int nbuffers = (int)(this.file.length() / Unit.SIZE); 
            MappedByteBuffer mmf = raf.getChannel().map(MapMode.READ_ONLY, 0, Unit.SIZE * nbuffers);
            this.mmfs.add(mmf);
            mmf.order(ByteOrder.nativeOrder());
            for (int i=0; i<nbuffers; i++) {
                Unit ii = new Unit();
                mmf.position(i * Unit.SIZE);
                mmf.limit(mmf.position() + Unit.SIZE);
                ii.buf = mmf.slice();
                ii.addr = UberUtil.getAddress(ii.buf);
                this.units.add(ii);
            }
            this.ch.close();
            this.raf.close();
        }
    }
    
    public static void log(Object... args) {
        if (_singlton != null) {
            _singlton.log_(args);
        }
    }
    
    public void log_(Object... args) {
        // get a buffer
        Unit unit = addr.get();
        if (unit == null) {
            return;
        }
        
        // write stuff
        unit.setThreadId();
        unit.setTimestamp();
        unit.buf.position(Unit.OFFSET_VALUES);
        try {
            unit.buf.put((byte)args.length);
            for (Object i:args) {
                writeValue(unit.buf, i);
            }
        }
        catch (Exception ignored) {}
    }
    
    public static synchronized Unit alloc_() {
        return (_singlton != null) ? _singlton.alloc() : null;
    }
    
    private synchronized Unit alloc() {
        for (int j=0; j<2; j++) {
            // _buffers[0] is the shared one
            for (int i=1; i<this.units.size(); i++) {
                Unit ii = this.units.get(i);
                if (ii.owner != null) {
                    if (ii.owner.isAlive()) {
                        continue;
                    }
                }
                ii.owner = Thread.currentThread();
                return ii;
            }
            grow();
        }
        return null;
    }
    
    private synchronized void grow() {
        try {
            long start = this.units.size() * Unit.SIZE;
            MappedByteBuffer mmf = this.ch.map(MapMode.READ_WRITE, start, Unit.SIZE * 100);
            this.mmfs.add(mmf);
            mmf.order(ByteOrder.nativeOrder());
            for (int i=0; i<100; i++) {
                Unit ii = new Unit();
                mmf.position(i * Unit.SIZE);
                mmf.limit(mmf.position() + Unit.SIZE);
                ii.buf = mmf.slice();
                ii.addr = UberUtil.getAddress(ii.buf);
                this.units.add(ii);
            }
        }
        catch (Exception ignored) {
            // if it fails then nothing is logged
        }
    }

    private static Object getValue(ByteBuffer buf) {
        Object result = null;
        byte type = buf.get();
        if (type == TypeId.NULL) {
            result = null;
        }
        else if (type == TypeId.BYTE) {
            result = buf.get();
        }
        else if (type == TypeId.INT) {
            result = buf.getInt();
        }
        else if (type == TypeId.FLOAT) {
            result = buf.getFloat();
        }
        else if (type == TypeId.LONG) {
            result = buf.getLong();
        }
        else if (type == TypeId.DOUBLE) {
            result = buf.getDouble();
        }
        else if (type == TypeId.TIMESTAMP) {
            result = new Timestamp(buf.getLong());
        }
        else if (type == TypeId.STRING) {
            ByteBuffer subset = buf.asReadOnlyBuffer();
            while (buf.hasRemaining() && buf.get() != 0) {
            }
            subset.limit(buf.position()-1);
            result = Charsets.UTF_8.decode(subset).toString();
        }
        else if (type == TypeId.BINARY) {
            int length = buf.getInt();
            byte[] bytes = new byte[length];
            for (int i=0; i<length; i++) {
                bytes[i] = buf.get();
            }
            result = bytes;
        }
        else if (type == TypeId.UNKNOWN) {
            result = "unknown type: " + type;
        }
        else {
            result = "not implemented: " + type;
        }
        return result;
    }
    
    private static Object[] getValues(ByteBuffer buf) {
        int length = buf.get();
        Object result[] = new Object[length];
        try {
            for (int i=0; i<length; i++) {
                result[i] = getValue(buf);
            }
        }
        catch (Exception x) {}
        return result;
    }
    
    private static void writeValue(ByteBuffer buf, Object value) {
        if (value == null) {
            buf.put(TypeId.NULL);
        }
        else if (value instanceof Byte) {
            buf.put(TypeId.BYTE);
            buf.put((byte)value);
        }
        else if (value instanceof Integer) {
            buf.put(TypeId.INT);
            buf.putInt((int)value);
        }
        else if (value instanceof Float) {
            buf.put(TypeId.FLOAT);
            buf.putFloat((float)value);
        }
        else if (value instanceof Long) {
            buf.put(TypeId.LONG);
            buf.putLong((long)value);
        }
        else if (value instanceof Double) {
            buf.put(TypeId.DOUBLE);
            buf.putDouble((double)value);
        }
        else if (value instanceof Date) {
            buf.put(TypeId.TIMESTAMP);
            buf.putLong(((Date)value).getTime());
        }
        else if (value instanceof String) {
            buf.put(TypeId.STRING);
            Utf8.encode((String)value, it->{
                if (buf.hasRemaining()) {
                    buf.put((byte)it);
                }
            });
            buf.put((byte)0);
        }
        else if (value instanceof byte[]) {
            byte[] bytes = (byte[])value;
            buf.put(TypeId.BINARY);
            buf.putInt(bytes.length);
            for (byte i:bytes) {
                if (buf.hasRemaining()) {
                    buf.put(i);
                }
            }
        }
        else if (value instanceof ByteBuffer) {
            ByteBuffer copy = ((ByteBuffer)value).asReadOnlyBuffer();
            buf.put(TypeId.BINARY);
            buf.putInt(copy.remaining());
            while (copy.hasRemaining()) {
                buf.put(copy.get());
            }
        }
        else {
            buf.put(TypeId.UNKNOWN);
        }
    }
    
    public Unit getUnit(int idx) {
        return this.units.get(idx);
    }
    
    public List<Unit> getUnits() {
        return this.units;
    }
    
    public void close() {
        IOUtils.closeQuietly(this.ch);
        IOUtils.closeQuietly(this.raf);
        for (MappedByteBuffer i:this.mmfs) {
            Unsafe.unmap(i);
        }
    }
}
