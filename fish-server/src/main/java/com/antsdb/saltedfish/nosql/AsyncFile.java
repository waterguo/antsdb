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
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.CodingError;
import com.antsdb.saltedfish.util.UberUtil;

class AsyncFile {
    static Logger _log = UberUtil.getThisLogger();
    static ExecutorService _closeService = Executors.newSingleThreadExecutor();
    
    File file;
    AsynchronousFileChannel ch;
    long pos = 0;
    long filesize;
    volatile boolean closeAfterWrite = false;
    AtomicInteger writeCount = new AtomicInteger();
    
    class MyHandler implements CompletionHandler<Integer, ByteBuffer> {
        int bytesToWrite;
        CompletionHandler<?,?> upstream;
        
        MyHandler(int bytesToWrite, CompletionHandler<?,?> upstream) {
            super();
            this.bytesToWrite = bytesToWrite;
            this.upstream = upstream;
        }

        @Override
        public void completed(Integer result, ByteBuffer buf) {
            _log.trace("written {} bytes", result);
            if (result != this.bytesToWrite) {
                failed(new Exception("bytes written is not the same as buffer size"), buf);
                return;
            }
            int count = writeCount.decrementAndGet();
            if (closeAfterWrite && (count == 0)) {
                closeForReal();
            }
            if (this.upstream != null) {
                this.upstream.completed(null, null);
            }
        }

        @Override
        public void failed(Throwable x, ByteBuffer attachment) {
            _log.error("unable to write to file " + file.getAbsolutePath(), x);
            if (this.upstream != null) {
                this.upstream.failed(null, null);
            }
        }
        
    }
    
    public AsyncFile(File file, long filesize) throws IOException {
        super();
        this.file = file;
        this.filesize = filesize;
        _log.debug("opening file " + file.getAbsolutePath());
        this.ch = AsynchronousFileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    }
    
    /**
     * 
     * @param buf
     */
    public void write(ByteBuffer buf, CompletionHandler<Object, Object> handler) {
        if (this.closeAfterWrite) {
            throw new CodingError("file is already closed");
        }
        
        // find out the max. number of bytes can be written
        
        int bytesToWrite = (int)Math.min(buf.remaining(), this.filesize - pos);
        ByteBuffer another = buf.asReadOnlyBuffer();
        another.limit(another.position() + bytesToWrite);
        
        // increase write count, prevent accidental closing
        
        this.writeCount.incrementAndGet();
        
        // close this file if it reaches the end
        
        if (bytesToWrite == this.filesize - pos) {
            this.closeAfterWrite = true;
        }
        
        // write to the file
        
        _log.trace("writting {} bytes", bytesToWrite);
        this.ch.write(another, this.pos, null, new MyHandler(bytesToWrite, handler));
        try {
            this.ch.force(true);
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        this.pos += bytesToWrite;
        
        // reposition the read pointer
        
        buf.position(buf.position() + bytesToWrite);
    }
    
    private void closeForReal() {
        _log.debug("closing file " + file.getAbsolutePath());
        try {
            ch.force(true);
        }
        catch (Exception x) {
            _log.error("unable to force update file " + file.getAbsolutePath(), x);
        }
        try {
            ch.close();
        }
        catch (Exception x) {
            _log.warn("unable to close file " + file.getAbsolutePath(), x);
        }
    }
    
    boolean isClosed() {
        return this.closeAfterWrite;
    }

    public void close() {
        _log.debug("closing file: " + file.getAbsolutePath());
        
        // prevent further write operations
        
        this.closeAfterWrite = true;
        
        // wait for the on-going writes to end
        
        while (this.writeCount.get() != 0) {
            try {
                Thread.sleep(10);
            }
            catch (InterruptedException e) {
            }
        }
        
        // close the file
        
        if (this.ch.isOpen()) {
            try {
                this.ch.close();
            }
            catch (IOException e) {
            }
        }
    }
}
