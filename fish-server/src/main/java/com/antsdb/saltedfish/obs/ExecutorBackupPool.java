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
package com.antsdb.saltedfish.obs;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

public class ExecutorBackupPool {
    final static Logger _log = UberUtil.getThisLogger();
    
    private ThreadPoolExecutor pool;
    
    public ExecutorBackupPool(int poolSize) {
        ThreadFactory tf = new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setDaemon(true);
                return t;
            }
        };
        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();      
        pool = new ThreadPoolExecutor(
                poolSize, 
                poolSize,
                0L, 
                TimeUnit.MILLISECONDS,
                queue,
                tf
                );;
    }

    public ExecutorService getPool() {
        return pool;
    }
    
    public void shutdown() {
        if(pool!=null) {
            pool.shutdown();
        }
    }
    
    public boolean isIdle() {
        int queueSize = pool.getQueue().size();
        boolean result = queueSize == 0 ? true : false;
        return result;
    }
}
