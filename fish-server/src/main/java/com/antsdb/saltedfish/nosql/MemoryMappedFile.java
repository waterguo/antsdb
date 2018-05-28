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
package com.antsdb.saltedfish.nosql;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.util.UberFormatter;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author wgu0
 */
public class MemoryMappedFile implements Closeable {
    static Logger _log = UberUtil.getThisLogger();
    
    File file;
    int size;
    long addr;
    MappedByteBuffer buf;
    FileChannel channel;
    RandomAccessFile raf;
    
    /**
     * 
     * @param file
     * @param mode "rw" for read and write. "r" for read only
     * @throws IOException
     */
    public MemoryMappedFile(File file, String mode) throws IOException {
        this(file, file.length(), mode);
    }
    
    public MemoryMappedFile(File file, long size, String mode) throws IOException {
        this(file, mode, 0, size);
    }
    
    public MemoryMappedFile(File file, String mode, long offset, long size) throws IOException {
        if (size >= Integer.MAX_VALUE) {
            throw new IllegalArgumentException("jvm doesn't support mapped file more than 2g");
        }
        MapMode mapmode = null;
        if (mode.equals("r")) {
            mapmode = MapMode.READ_ONLY;
        }
        else if (mode.equals("rw")) {
            mapmode = MapMode.READ_WRITE;
        }
        else {
            throw new IllegalArgumentException();
        }
        
        if ((mapmode == MapMode.READ_WRITE) && !file.exists()) {
            File parent = file.getAbsoluteFile().getParentFile();
            long free = parent.getUsableSpace();
            if ((size * 4) > free) {
                throw new HumpbackException("out of storage space: " + file.toString() + ' ' + free);
            }
        }
        
        this.file = file;
        boolean exist = this.file.exists();
        this.raf = new RandomAccessFile(file, mode);
        this.channel = raf.getChannel();
        this.buf = channel.map(mapmode, offset, size);
        this.buf.order(ByteOrder.nativeOrder());
        this.addr = UberUtil.getAddress(buf);
        if (this.addr == 0) {
            throw new IllegalArgumentException();
        }
        _log.debug(String.format("mounted %s %s %s at 0x%016x with length 0x%08x",
                exist ? "exist" : "new",
                file.toString(), 
                mode, 
                addr, 
                size));
    }

    public void close() {
        unmap();
        try {
            this.channel.close();
            this.raf.close();
        }
        catch (IOException e) {
            _log.warn("fail to close channel {}", this.file);
        }
        this.channel = null;
        this.raf = null;
    }
    
    public long getAddress() {
        return this.addr;
    }
    
    public int getSize() {
        return this.size;
    }

    public void force() {
        this.buf.force();
    }

    private void unmap() {
        _log.debug("{} @ {} is unmounted", file.toString(), UberFormatter.hex(addr));
        Unsafe.unmap(this.buf);
        this.buf = null;
        this.addr = 0;
    }
    
    public boolean isReadOnly() {
        return this.buf.isReadOnly();
    }

    @Override
    public String toString() {
        return this.file.toString();
    }

    public void force(long offset, int length) throws IOException {
        _log.debug(String.format("forcing %s offset 0x%08x length 0x%08x", file.toString(), offset, length));
        MappedByteBuffer buff = null;
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            FileChannel channel = raf.getChannel();
            buff = channel.map(MapMode.READ_WRITE, offset, size);
            buff.force();
        }
    }
    
}
