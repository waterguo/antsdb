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
package com.antsdb.saltedfish.cpp;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;

import com.antsdb.saltedfish.util.SizeConstants;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class FileBasedHeap extends FlexibleHeap {
    /** use file to back up the keep only if consumptions goes beyond this size */
    static final long MEMORY_THRESHOLD = SizeConstants.mb(8);
    static long _memory_limit = SizeConstants.gb(1);
    static final Logger _log = UberUtil.getThisLogger();
    
    private File file = null;
    private File home;
    private long fileStartPos;
    private RandomAccessFile raf;
    private FileChannel ch;
    
    public FileBasedHeap(File home) {
        this.home = home;
    }
    
    public static void setLimit(long value) {
        _memory_limit = value;
    }
    
    @Override
    protected Block createBlock(int size) {
        try {
            long cap = getCapacity();
            if (cap < MEMORY_THRESHOLD) {
                return super.createBlock(size);
            }
            if (cap > _memory_limit) {
                throw new OutOfHeapException(String.valueOf(cap));
            }
            if (this.file == null) {
                this.file = File.createTempFile("heap-", null, this.home);
                _log.debug("creating temporary file {} ...", this.file);
                this.fileStartPos = this.tail.startPosition + this.tail.buffer.capacity();
                this.raf = new RandomAccessFile(this.file, "rw");
                this.ch = this.raf.getChannel();
            }
            long blockStartPos = cap;
            long pos = blockStartPos - this.fileStartPos;
            Block result = new Block(this.ch.map(MapMode.READ_WRITE, pos, size));
            return result;
        }
        catch (IOException x) {
            throw new IllegalArgumentException(x);
        }
    }

    @Override
    public void free() {
        // free memory
        for (Block i=this.head; i!=null; i=i.next) {
            if (i.isFromMemoryManager) {
                MemoryManager.free(i.buffer);
            }
            else {
                Unsafe.unmap(i.buffer);
            }
        }
        
        // free files
        IOUtils.closeQuietly(this.raf);
        IOUtils.closeQuietly(this.ch);
        this.raf = null;
        this.ch = null;
        if (this.file != null) {
            _log.debug("deleting temporary file {} cap={} ...", this.file, getCapacity());
            if (!this.file.delete()) {
                _log.warn("unable to delete temporary file: {}", this.file);
            }
            this.file = null;
        }
        
        // misc
        this.head = null;
        this.tail = null;
        this.current = null;
    }
}
