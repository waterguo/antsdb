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

import java.nio.CharBuffer;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;

import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class AsynchronusInsert implements AutoCloseable {
    private static Logger _log = UberUtil.getThisLogger();
    
    private ThreadPoolExecutor executor;
    private Session session;
    private AtomicReference<Exception> error = new AtomicReference<Exception>(null);
    private String sql;
    private Transaction trx;
    private int count;
    
    private class Task implements Runnable {
        
        CharBuffer cs;
        
        public Task(CharBuffer cbuf) {
            this.cs = cbuf;
        }

        public void run() {
            // if something bad has happened, stop
            
            if (AsynchronusInsert.this.error.get() != null) {
                return;
            }
            
            try {
                Script script = session.parse(this.cs);
                Instruction step = script.getRoot();
                VdmContext ctx = new VdmContext(session, script.getVariableCount()); 
                step.run(ctx, new Parameters(), 0);
            }
            catch (Exception x) {
                if (AsynchronusInsert.this.error.compareAndSet(null, x)) {
                    AsynchronusInsert.this.sql = cs.toString();
                }
            }
        }
    }
    
    public AsynchronusInsert(Session session) {
        this.session = session;
        this.trx = session.getTransaction();
        trx.makeAutonomous();
        int ncpu = Runtime.getRuntime().availableProcessors();
        executor = new ThreadPoolExecutor(0, ncpu, 1, TimeUnit.MINUTES, new SynchronousQueue<>());
        executor.setRejectedExecutionHandler(new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                try {
                    if (!executor.isShutdown()) {
                        executor.getQueue().put(r);
                    }
                } 
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RejectedExecutionException("interrupted", e);
                }            
            }
        });
    }
    
    public void add(CharBuffer cs) {
        this.count++;
        this.executor.submit(new Task(cs));
    }

    public void waitForCompletion() {
        while (this.count != this.executor.getCompletedTaskCount()) {
            UberUtil.sleep(100);
        }
    }
    
    @Override
    public void close() {
        this.executor.shutdown();
        try {
            if (!this.executor.awaitTermination(1, TimeUnit.HOURS)) {
                _log.warn("timeout waiting");
            }
        }
        catch (InterruptedException e) {
            throw new OrcaException(e);
        }
        if (getError() != null) {
            throw new OrcaException(getError(), this.sql);
        }
    }

    public Exception getError() {
        return this.error.get();
    }
}
