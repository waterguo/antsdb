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
package com.antsdb.saltedfish.util;

import java.lang.reflect.Field;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

/**
 * schedules background jobs
 *  
 * @author wgu0
 */
public class FishJobManager {
    static Logger _log = UberUtil.getThisLogger();
    static Field _hiddenField;
    
    ScheduledThreadPoolExecutor poolPeriodic = new ScheduledThreadPoolExecutor(2);
    ScheduledThreadPoolExecutor poolOneTime = new ScheduledThreadPoolExecutor(1);
    
    static {
        try {
            _hiddenField = FutureTask.class.getDeclaredField("callable");
            _hiddenField.setAccessible(true);
        }
        catch (Exception e) {
            _log.error("", e);
        }
    }
    public FishJobManager() {
    }
    
    public ScheduledFuture<?> scheduleWithFixedDelay(long delay, TimeUnit unit, Runnable callable) {
        return poolPeriodic.scheduleWithFixedDelay(callable, 0, delay, unit);
    }

    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable callable, long delay, TimeUnit unit) {
        return poolPeriodic.scheduleWithFixedDelay(callable, 0, delay, unit);
    }

    public ScheduledFuture<?> schedule(long delay, TimeUnit unit, Runnable callable) {
        return poolOneTime.schedule(callable, delay, unit);
    }

    public ScheduledFuture<?> schedule(long delay, TimeUnit unit, Callable<?> callable) {
        return poolOneTime.schedule(callable, delay, unit);
    }

    public void close() {
        // shutdown periodic futures
        this.poolPeriodic.shutdownNow();
        this.poolOneTime.shutdownNow();
        
        // shut
        try {
            this.poolPeriodic.awaitTermination(10, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
        }
        
        // shutdown one-shot futures. we need to complete these futures
        try {
            this.poolOneTime.awaitTermination(10, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
        }
        
        // log stuck jobs
        for (Runnable i:this.poolPeriodic.getQueue()) {
            _log.warn("stuck job: {}", i);
        }
        for (Runnable i:this.poolOneTime.getQueue()) {
            _log.warn("stuck job: {}", i);
        }
    }
}
