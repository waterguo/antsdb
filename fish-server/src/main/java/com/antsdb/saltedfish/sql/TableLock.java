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
package com.antsdb.saltedfish.sql;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.CodingError;
import com.antsdb.saltedfish.util.UberTimer;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author wgu0
 */
public final class TableLock {
    static Logger _log = UberUtil.getThisLogger();
    
    AtomicInteger level = new AtomicInteger();
    TableLock next;
    volatile int owner;
    boolean transactional;
    int tableId;
    int sessionId;
    
    public TableLock(int sessionId, int tableId) {
        this.tableId = tableId;
        this.sessionId = sessionId;
    }
    
    /**
     * 
     * @param owner
     * @param level
     * @param timeout
     * @return false if an existing lock has higher level
     */
    public boolean acquire(int owner, int level, int timeout) {
        UberTimer timer = new UberTimer(timeout);
        for (;;) {
            int curLevel = this.level.get();

            // upgrade the lock ?
            
            if ((this.owner == 0) || (this.owner == owner)) {
                if (level <= curLevel) {
                    return false;
                }
                if (!this.level.compareAndSet(curLevel, level)) {
                    // concurrency , retry
                    continue;
                }
                this.owner = owner;
                return true;
            }
            
            // is lock hold by other sessions ?
            
            if (timer.isExpired()) {
                throw new OrcaException(
                        "time out acquiring lock {} requester={} request_level={}",
                        toString(),
                        owner,
                        level);
            }
            try {
                Thread.sleep(1);
            }
            catch (InterruptedException e) {
            }
            continue;
        }
    }
    
    public void makeTransactional(TableLock next) {
        if (this.next != null) {
            throw new CodingError();
        }
        this.next = next;
        this.transactional = true;
    }

    public TableLock getNext() {
        return this.next;
    }

    public void releaseAll() {
        if (this.next != null) {
            this.next.releaseAll();
        }
        for (TableLock i = this; i!=null; i=i.next) {
            int state = i.level.get();
            if (!i.transactional) {
                continue;
            }
            if (state >= LockLevel.EXCLUSIVE) {
                // very likely a bug. transactional locks should be SHARED
                throw new CodingError();
            }
            release(this.owner);
        }
    }

    public int getLevel() {
        return this.level.get();
    }

    public boolean isTransactional() {
        return transactional;
    }

    public void release(int owner) {
        if (this.owner != owner) {
            // it is possible that owners are not the same. if this is in the undo process of lock()
            return;
        }
        this.owner = 0;
        this.transactional = false;
        this.next = null;
        this.level.set(LockLevel.NONE);
    }

    public void setTransactional(boolean b) {
        this.transactional = b;
    }

    @Override
    public String toString() {
        return String.format(
                "%d:%d owner=%d level=%d transactional=%b",
                this.sessionId,
                this.tableId,
                this.owner, 
                this.level.get(), 
                this.transactional);
    }

    public TableLock clone(int sessionId) {
        TableLock result = new TableLock(sessionId, tableId);
        result.level.set(this.level.get());
        result.owner = this.owner;
        result.transactional = this.transactional;
        return result;
    }
}
