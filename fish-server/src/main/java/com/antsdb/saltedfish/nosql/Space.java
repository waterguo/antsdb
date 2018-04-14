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
package com.antsdb.saltedfish.nosql;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * a single unit lives on the storage. 
 *  
 * @author wgu0
 */
final class Space implements AutoCloseable {
    static Logger _log = UberUtil.getThisLogger();
    
    int id;
    File file;
    long addr;
    long spStart;
    long spEnd;
    AccessToken token;
    AtomicInteger allocPointer;
    MemoryMappedFile mmf;
    int capacity;
    boolean isGarbage = false;
    
    public void open(MapMode mode, int fileSize) throws IOException {
        int fileLength = (int)this.file.length();
        int size = (mode == MapMode.READ_WRITE) ? fileSize : fileLength;
        this.mmf = new MemoryMappedFile(this.file, size, mode == MapMode.READ_WRITE ? "rw" : "r");
        if (mode == MapMode.READ_WRITE) {
            File parent = file.getAbsoluteFile().getParentFile();
            long free = parent.getUsableSpace();
            if ((fileSize * 4) > free) {
                throw new HumpbackException("out of storage space: " + this.file.toString() + ' ' + free);
            }
        }
        if (mode == MapMode.READ_WRITE) {
            this.mmf.buf.load();
        }
        this.mmf.buf.order(ByteOrder.LITTLE_ENDIAN);
        this.addr = this.mmf.getAddress();
        this.spStart = SpaceManager.makeSpacePointer(this.id, 0);
        this.spEnd = fileSize + this.spStart;
        this.allocPointer = new AtomicInteger(fileLength);
        this.capacity = this.mmf.buf.capacity();
    }
    
    @Override
    public void close() throws Exception {
        if (this.mmf == null) {
            return;
        }
        this.mmf.close();
        this.mmf = null;
        this.addr = 0;
    }
    
    void delete() throws Exception {
        close();
        HumpbackUtil.deleteHumpbackFile(this.file);
        _log.debug("{} is deleted", this.file);
    }

    public int getCapacity() {
        return this.capacity;
    }

    public void force(long offsetStart, long size) throws IOException {
        MappedByteBuffer buf = this.mmf.channel.map(MapMode.READ_WRITE, offsetStart, size);
        buf.force();
        Unsafe.unmap(buf);
    }

    public void resize() throws IOException {
        if (this.allocPointer == null) {
            return;
        }
        int size = this.allocPointer.get();
        if (size == this.file.length()) {
            return;
        }
        RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
        raf.setLength(size);
        raf.close();
    }

}
