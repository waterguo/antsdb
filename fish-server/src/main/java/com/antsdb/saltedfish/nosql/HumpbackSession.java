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

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.util.UberTime;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * represents a humpback session
 *  
 * @author *-xguo0<@
 */
public final class HumpbackSession implements Closeable{
    static Logger _log = UberUtil.getThisLogger();
    static AtomicInteger _nextId = new AtomicInteger(1);
    
    String endpoint;
    volatile long ts;
    int id = 0;
    private volatile Transaction trx = new Transaction();
    private Humpback humpback;
    private volatile long lpStart;
    private long lastLp;
    
    public HumpbackSession(Humpback humpback, String endpoint) {
        this.humpback = humpback;
        this.endpoint = endpoint;
        // make sure id > 0
        for (;;) {
            this.id = _nextId.getAndIncrement();
            if (this.id > 0) {
                break;
            }
            if (this.id < 0) {
                _nextId.compareAndSet(this.id, 1);
            }
        }
    }
    
    public HumpbackSession open() {
        ts = UberTime.getTime();
        return this;
    }
    
    @Override
    public void close() {
        this.lpStart = 0;
        this.ts = 0;
    }

    public long getOpenTime() {
        return this.ts;
    }

    public int getId() {
        return this.id;
    }

    public String getEndpoint() {
        return this.endpoint;
    }
    
    @Override
    public String toString() {
        return "hsession: " + this.id;
    }

    public void negateId() {
        this.id = -this.id;
    }
    
    public Transaction getTransaction() {
        return this.trx;
    }
    
    /**
     * commit current active transaction
     */
    public long commit() {
        long trxts = 0;
        Transaction trx = getTransaction();
        long trxid = trx.getTrxId();
        if (trxid < 0) {
            trxts = TrxMan.getNewVersion();
            Gobbler gobbler = this.humpback.getGobbler();
            if (gobbler != null) {
                _log.trace("commit trxid={} trxts={}", trxid, trxts);
                gobbler.logCommit(this, trxid, trxts);
            }
            this.humpback.trxMan.commit(trxid, trxts);
        }
        trx.reset();
        this.lpStart = 0;
        return trxts;
    }

    /**
     * roll back current active transaction
     */
    public void rollback() {
        Gobbler gobbler = this.humpback.getGobbler();
        long trxid = trx.getTrxId();
        if (trxid < 0) {
            _log.trace("rollback trxid={}", trxid);
            if (gobbler != null) {
                gobbler.logRollback(this, trxid);
            }
            this.humpback.trxMan.rollback(trxid);
        }
        trx.reset();
        this.lpStart = 0;
    }
    
    /**
     * start log pointer of the potential upcoming updates
     * @return
     */
    public long getStartLp() {
        return this.lpStart;
    }

    /**
     * prepare for any potential upcoming database updates
     */
    public void prepareUpdates() {
        if (this.lpStart == 0) {
            this.lpStart = this.humpback.getSpaceManager().getAllocationPointer();
        }
    }

    public void setLastLp(long lp) {
        this.lastLp = lp;
    }
    
    public long getLastLp() {
        return this.lastLp;
    }
}
