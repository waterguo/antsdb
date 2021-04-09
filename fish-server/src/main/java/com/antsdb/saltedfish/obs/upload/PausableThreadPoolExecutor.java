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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

public class PausableThreadPoolExecutor extends ThreadPoolExecutor {
    final static Logger _log = UberUtil.getThisLogger();

    private boolean isPaused; // 标志是否被暂停
    private ReentrantLock pauseLock = new ReentrantLock(); // 访问isPaused时需要加锁，保证线程安全
    private Condition unpaused = pauseLock.newCondition();
    
    public PausableThreadPoolExecutor(
            int corePoolSize, 
            int maximumPoolSize, 
            long keepAliveTime, 
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue,
            ThreadFactory factory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,factory);
    }

    // beforeExecute为ThreadPoolExecutor提供的hood方法
    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
        pauseLock.lock();
        try {
            while (isPaused)
                unpaused.await();
        }
        catch (InterruptedException ie) {
            t.interrupt();
        }
        finally {
            pauseLock.unlock();
        }
    }

    // 暂停
    public void pause() {
        pauseLock.lock();
        try {
            isPaused = true;
        }
        finally {
            pauseLock.unlock();
        }
    }

    // 取消暂停
    public void resume() {
        pauseLock.lock();
        try {
            isPaused = false;
            unpaused.signalAll();
        }
        finally {
            pauseLock.unlock();
        }
    }

}
