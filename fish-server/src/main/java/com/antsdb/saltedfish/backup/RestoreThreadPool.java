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
package com.antsdb.saltedfish.backup;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
class RestoreThreadPool {
    static final Logger _log = UberUtil.getThisLogger();
    static final List<Object[]> EOF = Collections.emptyList();
    
    private int nThreads;
    private BlockingQueue<List<Object[]>> queue;
    List<RestoreThread> threads = new ArrayList<>();
    private List<Object[]> rows;
    private int batchSize;
    
    RestoreThreadPool(int nThreads, int batchSize) {
        this.nThreads = nThreads;
        this.batchSize = batchSize;
    }
    
    void start(Supplier<Connection> factory) {
        this.queue = new ArrayBlockingQueue<>(this.nThreads * 2);
        for (int i=0; i<this.nThreads; i++) {
            RestoreThread thread = new RestoreThread(factory.get(), this.queue);
            thread.setName("restore-worker-" + i);
            thread.start();
            this.threads.add(thread);
        }
    }

    void waitForCompletion() throws InterruptedException {
        // wait for queue to be emptied
        
        while (!this.queue.isEmpty()) {
            Thread.sleep(100);
        }
        
        // wait for threads 
        
        for (RestoreThread i:this.threads) {
            while (i.isBusy) {
                Thread.sleep(100);
            }
        }
        
        // check errors
        
        for (RestoreThread i:this.threads) {
            if (i.error != null) {
                throw new RuntimeException(i.error);
            }
        }
        
    }
    
    void close() throws InterruptedException {
        // send end signal
        
        this.queue.put(EOF);
        
        // wait until all threads ended
        
        for (RestoreThread i:this.threads) {
            try {
                i.join(5000);
                if (i.isAlive()) {
                    _log.warn("unable to stop thread {}", i);
                }
            }
            catch (InterruptedException e) {
            }
        }
        
        // done
        
        this.threads.clear();
    }
    
    void send(Object[] row) throws InterruptedException {
        if (this.rows == null) {
            this.rows = new ArrayList<>(this.batchSize);
        }
        this.rows.add(row);
        flushWhenFull();
    }
    
    void send(List<Object[]> rows) throws InterruptedException {
        for (RestoreThread i:this.threads) {
            if (i.error != null) {
                throw new RuntimeException(i.error);
            }
        }
        this.queue.put(this.rows);
    }
    
    private void flushWhenFull() throws InterruptedException {
        if (this.rows.size() >= this.batchSize) {
            send(this.rows);
            this.rows = null;
        }
    }
    
    void flush() throws InterruptedException {
        if ((this.rows != null) && (this.rows.size() != 0)) {
            send(this.rows);
            this.rows = null;
        }
    }

    void prepare(String sql) throws SQLException {
        for (RestoreThread i:this.threads) {
            i.prepare(sql);
        }
    }
    
    long getCount() {
        long result = 0;
        for (RestoreThread i:this.threads) {
            result += i.count;
        }
        return result;
    }

    public void clear() {
        this.rows = null;
        for (RestoreThread i:this.threads) {
            i.error = null;
        }
    }
}
