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
package com.antsdb.saltedfish.sql.vdm;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;

import com.antsdb.mysql.network.PacketUtil;
import com.antsdb.saltedfish.charset.Decoder;
import com.antsdb.saltedfish.cpp.AllocPoint;
import com.antsdb.saltedfish.cpp.MemoryManager;
import com.antsdb.saltedfish.sql.CharBufferStream;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.sql.mysql.MysqlParserFactory;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * improve the import performance by parallelizing the inserts
 *   
 * @author *-xguo0<@
 */
public class AsynchronousInsert implements AutoCloseable {
    private static Logger _log = UberUtil.getThisLogger();
    
    private ThreadPoolExecutor executor;
    private Session session;
    private AtomicReference<Exception> error = new AtomicReference<Exception>(null);
    private String errorSql;
    private Transaction trx;
    private int count;
    private Decoder decoder;
    private BlockingQueue<Task> pool;
    
    private class Task implements Runnable {
        
        ByteBuffer sql;
        
        void assign(ByteBuffer input) {
            if (this.sql != null) {
                this.sql.clear();
            }
            this.sql = MemoryManager.growImmortal(AllocPoint.ASYNCHRONOUS_INSERT, this.sql, input.remaining());
            this.sql.put(input);
            this.sql.flip();
        }
        
        void close() {
            MemoryManager.freeImmortal(AllocPoint.ASYNCHRONOUS_INSERT, this.sql);
            this.sql = null;
        }
        
        @Override
        public void run() {
            try {
                run0();
            }
            finally {
                AsynchronousInsert.this.pool.offer(this);
            }
        }
        
        public void run0() {
            // if something bad has happened, stop
            if (AsynchronousInsert.this.error.get() != null) {
                return;
            }
            
            // the heavy lift
            CharBuffer chars = toCharBuffer(this.sql);
            try {
                AsynchronousInsert.this.session.getOrca().getScheduler().getUserLoadCounter().incrementAndGet();
                MysqlParserFactory parser = (MysqlParserFactory)session.getParserFactory();
                Script script = parser.parse(null, session, new CharBufferStream(chars), 1);
                Instruction step = script.getRoot();
                VdmContext ctx = new VdmContext(session, script.getVariableCount()); 
                step.run(ctx, new Parameters(), 0);
            }
            catch (Exception x) {
                if (AsynchronousInsert.this.error.compareAndSet(null, x)) {
                    AsynchronousInsert.this.errorSql = chars.toString();
                }
            }
            finally {
                AsynchronousInsert.this.session.getOrca().getScheduler().getUserLoadCounter().decrementAndGet();
            }
        }
    }
    
    public AsynchronousInsert(Session session) {
        this.session = session;
        this.decoder = session.getConfig().getRequestDecoder();
        this.trx = session.getTransaction();
        trx.makeAutonomous();
        int ncpu = this.session.getOrca().getConfig().getAsyncInsertThreads();
        _log.debug("start asynchronous import with {} threads", ncpu);
        this.pool = new ArrayBlockingQueue<>(ncpu);
        executor = new ThreadPoolExecutor(ncpu, ncpu, 1, TimeUnit.MINUTES, new ArrayBlockingQueue<>(ncpu));
        for (int i=0; i<ncpu; i++) {
            this.pool.add(new Task());
        }
    }
    
    public Integer submit(ByteBuffer sql) {
        if (this.error.get() != null) {
            throw new OrcaException(this.error.get());
        }
        Task task;
        try {
            task = this.pool.take();
        }
        catch (InterruptedException x) {
            throw new OrcaException(x);
        }
        task.assign(sql);
        this.executor.submit(task);
        this.count++;
        return 1;
    }

    public void waitForCompletion() {
        while (this.count != this.executor.getCompletedTaskCount()) {
            UberUtil.sleep(100);
        }
    }
    
    @Override
    public void close() {
        // free memory
        while (this.pool.peek() != null) {
            Task task = this.pool.poll();
            task.close();
        }
        
        // free threads
        this.executor.shutdown();
        try {
            if (!this.executor.awaitTermination(1, TimeUnit.HOURS)) {
                _log.warn("timeout waiting");
            }
        }
        catch (InterruptedException e) {
            throw new OrcaException(e);
        }
        
        // error handling
        if (getError() != null) {
            throw new OrcaException(getError(), this.errorSql);
        }
    }

    private Exception getError() {
        return this.error.get();
    }
    
    private CharBuffer toCharBuffer(ByteBuffer buf) {
        long pSql = UberUtil.getAddress(buf);
        CharBuffer result = PacketUtil.readStringAsCharBufWithMysqlExtension(pSql, buf.limit(), this.decoder);
        result.flip();
        return result;
    }
    
}
