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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.WriteAheadLog.MyFilenameFilter;
import com.antsdb.saltedfish.util.CodingError;
import com.antsdb.saltedfish.util.UberUtil;

import static com.antsdb.saltedfish.nosql.WriteAheadLog.*;

/**
 * assumes single threaded
 * 
 * @author xguo
 *
 */
class WriteAheadLogThread extends Thread{
    static final long LOG_FILE_SIZE = 1024*1024*1024l;
    static final int BUFFER_SIZE = 4*1024*1024;
    static final int BUFFERS = 4;
    static final Object EOF_MARK = "EOF";
    
    static Logger _log = UberUtil.getThisLogger();

    File home;
    File cp;
    List<File> logs = new ArrayList<File>();
    Queue<Object> queue = new ConcurrentLinkedQueue<>();
    AtomicInteger writeCount = new AtomicInteger();
    ByteBuffer buf;
    BlockingQueue<ByteBuffer> buffers = new LinkedBlockingQueue<ByteBuffer>();
    AsyncFile currentFile;
    long logFileSize = LOG_FILE_SIZE;

    static enum EntryType {
        ROW,
        COMMIT,
        ROLLBACK,
    }
    
    WriteAheadLogThread(File home) throws Exception {
        setName("wal");
        this.home = home;

        if (!home.isDirectory()) {
            throw new HumpbackException("home directory is not found " + home);
        }
        
        // checkpoint file
        
        this.cp = new File(home, "wal.cp");
        
        // collect existing log files

        for (File i:home.listFiles(new MyFilenameFilter())) {
            String sequenceText = i.getName().substring(4, 6);
            int sequence = Integer.parseInt(sequenceText);
            while (this.logs.size() <= sequence) {
                this.logs.add(null);
            }
            this.logs.set(sequence, i);
        }
        
        // allocate buffers
        
        for (int i=0; i<BUFFERS; i++) {
            this.buffers.add(ByteBuffer.allocateDirect(BUFFER_SIZE));
        }
        
        // initialize members
        
        getBuffer(0);
    }
    
    @Override
    public void run() {
        try {
            mainLoop();
        }
        catch (Exception x) {
            _log.error("failers from wal thread", x);
        }
    }

    private void mainLoop() throws Exception {
        for (;;) {
            Object obj = this.queue.poll();
            
            // sleep a little if there is nothing to do 
            
            if (obj == null) {
                try {
                    flush();
                    Thread.sleep(10);
                    continue;
                }
                catch (InterruptedException e) {
                }
            }
            
            // eof ?
            
            if (obj == EOF_MARK) {
                flush();
                break;
            }
            
            // write the row
            
            if (obj instanceof Row) {
                Row row = (Row)obj;
                write(row);
            }
            else if (obj instanceof Commit) {
                write((Commit)obj);
            }
            else if (obj instanceof Rollback) {
                write((Rollback)obj);
            }
            else {
                throw new CodingError();
            }
        }
    }
    
    private void write(Commit commit) throws Exception {
        ByteBuffer buf = getBuffer(17);
        buf.put((byte)EntryType.COMMIT.ordinal());
        buf.putLong(commit.trxid);
        buf.putLong(commit.trxts);
    }
    
    private void write(Rollback rollback) throws Exception {
        ByteBuffer buf = getBuffer(9);
        buf.put((byte)EntryType.ROLLBACK.ordinal());
        buf.putLong(rollback.trxid);
    }
    
    private void write(Row row) throws Exception {
    	/*
        ByteBuffer bytes = row.getBytes();
        ByteBuffer buf = getBuffer(9);
        buf.put((byte)EntryType.ROW.ordinal());
        buf.putInt(row.tableId);
        buf.putInt(bytes.limit());
        write(bytes);
        flushIfFull();
        */
    }
    
    @SuppressWarnings("unused")
	private void write(ByteBuffer bytes) throws Exception {
        // find the number of bytes can fit into buffer
        
        ByteBuffer buf = getBuffer(0);
        bytes = bytes.asReadOnlyBuffer();
        bytes.position(0);
        int bytesToCopy = Math.min(bytes.remaining() , buf.capacity() - buf.position());
        
        // copy the bytes
        
        bytes.limit(bytesToCopy);
        buf.put(bytes);
        
        // keep writing if there are remaining bytes
        
        if (bytes.remaining() > 0) {
            write(bytes);
        }
    }
    
    /**
     * returned buffer at least have 8 bytes of free space
     * 
     * @return
     * @throws InterruptedException 
     */
    private ByteBuffer getBuffer(int minSpace) throws Exception {
        flushIfFull(minSpace);
        if (this.buf == null) {
            this.buf = this.buffers.take();
            this.buf.clear();
        }
        return this.buf; 
    }
    
    /**
     * buffer is considered full when it has less than 8 bytes
     * @throws IOException 
     */
    void flushIfFull(int minSpace) throws IOException {
        if (this.buf == null) {
            return;
        }
        if (this.buf.remaining() < minSpace) {
            flush();
        }
    }
    
    void flushIfFull() throws IOException {
        flushIfFull(0);
    }
    
    void flush() throws IOException {
        if (this.buf == null) {
            return;
        }
        this.buf.flip();
        for (; this.buf.remaining() > 0;) {
            AsyncFile file = getFile();
            ByteBuffer bufToWrite = this.buf;
            file.write(bufToWrite, new CompletionHandler<Object, Object>() {

                @Override
                public void completed(Object result, Object attachment) {
                    buffers.offer(bufToWrite);
                }

                @Override
                public void failed(Throwable exc, Object attachment) {
                    buffers.offer(bufToWrite);
                }
            });
        }
        this.buf = null;
    }
    
    private AsyncFile getFile() throws IOException {
        if ((this.currentFile==null) || (this.currentFile.isClosed())) {
            String fileName = String.format("wal-%03d.log", this.logs.size());
            File file = new File(this.home, fileName);
            this.logs.add(file);
            this.currentFile = new AsyncFile(file, this.logFileSize);
        }
        return this.currentFile;
    }
    
    void shutdown() {
        // send the thread the eof signal
        
        this.queue.offer(EOF_MARK);
        
        // wait the thread to die
        
        try {
            join();
        }
        catch (InterruptedException e) {
        }
        
        // close all opened files
        
        if (this.currentFile != null) {
            this.currentFile.close();
        }
    }
}
