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
package com.antsdb.saltedfish.obs.upload;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

public class ExecutorBoatPool {
    final static Logger _log = UberUtil.getThisLogger();
    
    public final static int QUEUE_SIZE = 10;
    private PausableThreadPoolExecutor pool;
    private BlockingQueue<Runnable> queue;
    
    public ExecutorBoatPool() {
        ThreadFactory tf = new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setDaemon(true);
                t.setName("FishBoat");
                return t;
            }
        };
        queue = new LinkedBlockingQueue<Runnable>(QUEUE_SIZE);      
        pool = new PausableThreadPoolExecutor(
                1, 
                1,
                0L, 
                TimeUnit.MILLISECONDS,
                queue,
                tf
                );
    }
 
    public PausableThreadPoolExecutor getPool() {
        return pool;
    }
    
    public void shutdown() {
        if(pool!=null) {
            pool.shutdown();
            cleanQueue();
            try {
                if(!pool.awaitTermination(2,TimeUnit.SECONDS)) {
                    pool.shutdownNow();
                    _log.debug("The boat mandatory by the shore");
                }
                else {
                    _log.debug("The boat by the shore");
                }
            }
            catch (InterruptedException e) {
                _log.error(e.getMessage(),e);
                pool.shutdownNow();
            }
        }
    }

    public void showPoolInfo() {
        int queueSize = pool.getQueue().size();
        long taskCount = pool.getTaskCount();
        long completedTaskCount = pool.getCompletedTaskCount();
        int activeCount = pool.getActiveCount();
        _log.trace("queueSize:{},taskCount:{},completedTaskCount:{},activeCount:{}",queueSize,taskCount,completedTaskCount,activeCount);
    }
    
    private void cleanQueue(){
        if(pool.getQueue().size() > 0) {
            pool.getQueue().clear();
        }
    }
    
}
