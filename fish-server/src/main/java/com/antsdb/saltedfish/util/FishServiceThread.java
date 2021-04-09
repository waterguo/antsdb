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

import java.io.InterruptedIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author *-xguo0<@
 */
public abstract class FishServiceThread implements Runnable {
    static final int RETRY_WAIT = 30 * 1000;
    static final int IDLE_WAIT = 10 * 1000;
    
    final Logger log;
    private volatile Exception error;
    private volatile int retries;
    private String name;
    private Thread thread;
    
    public FishServiceThread(String name) {
        this.name = name;
        log = LoggerFactory.getLogger(getClass().getName());
    }
    
    public String getName() {
        return this.name;
    }
    
    @Override
    public void run() {
        log.info("{} started ...", getName());
        for (;;) {
            if (Thread.currentThread().isInterrupted()) {
                break;
            }
            try {
                if (this.error != null) {
                    this.retries++;
                }
                service();
                this.error = null;
                UberUtil.sleep(IDLE_WAIT);
            }
            catch (InterruptedIOException x) {
                break;
            }
            catch (InterruptedException x) {
                break;
            }
            catch (Exception x) {
                log.warn("{} failed with error. retry later", getName(), x);
                this.error = x;
                UberUtil.sleep(RETRY_WAIT);
                continue;
            }
        }
        log.info("{} ended ...", getName());
    }

    protected abstract boolean service() throws Exception;

    public void start() {
        this.thread = new Thread(this, this.name);
        this.thread.setDaemon(true);
        this.thread.start();
    }
    
    public void close(boolean wait) {
        if (this.thread == null) {
            return;
        }
        this.thread.interrupt();
        if (wait) {
            while (this.thread.isAlive()) {
                UberUtil.sleep(100);
            }
            this.thread = null;
        }
    }
    
    public int getRetryCount() {
        return this.retries;
    }
    
    public Exception getError() {
        return this.error;
    }
    
    public boolean isAlive() {
        if (this.thread != null) {
            return this.thread.isAlive();
        }
        return false;
    }
    
    public void stop() {
        if (this.thread != null) {
            this.thread.interrupt();
        }
    }
}
