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

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.IdGenerator;
import com.antsdb.saltedfish.util.UberUtil;

public final class TrxMan {
    static Logger _log = UberUtil.getThisLogger();

    public final static int MARK_ROLLED_BACK = -1;
    final static int MARK_ROW_LOCK = -3;
    
    Map<Long, Long> trx = new ConcurrentHashMap<Long, Long>(1000);
    private static AtomicLong _trxid = new AtomicLong(-IdGenerator.getId());
    private static AtomicLong _version = new AtomicLong(IdGenerator.getId());
    private boolean isClosed;
    private volatile long oldest = -10;
    
    public TrxMan(SpaceManager sm) {
    }
    
    /**
     * convert transaction id to transaction time stamp
     * 
     * @param trx
     * @return trx if the transaction is not committed
     */
    public final long getTimestamp(long trx) {
        if (trx >= MARK_ROLLED_BACK) {
            return trx;
        }
        
        // if we know the trx is committed , return the version
        Long timestamp = this.trx.get(trx);
        if (timestamp != null) {
            return timestamp;
        }
        
        // this is not supposed to happen unless in recovery mode.
        if (trx > this.oldest) {
            if (this.isClosed) {
                // recoverer is depending this logic. if there are unfinished transaction in the log
                return MARK_ROLLED_BACK;
            }
            throw new IllegalArgumentException("oldest=" + this.oldest + " trx=" + trx);
        }
        
        // transaction hasnt been committed, return the trxid
        return trx;
    }

    public void commit(long trxid, long trxts) {
        if (_log.isTraceEnabled()) {
            _log.trace("commit trxid={} trxts={}", trxid, trxts);
        }
        if (trxid > this.oldest) {
            throw new IllegalArgumentException("oldest=" + this.oldest + " trx=" + trxid);
        }
        // reset version if incoming version is greater than version counter. this could happen in slave receiving 
        // replication data 
        if (trxts > _version.get()) {
            _version.set(trxts);
        }
        this.trx.put(trxid, trxts);
    }

    public void rollback(long trxid) {
        if (_log.isTraceEnabled()) {
            _log.trace("rollback trxid={}", trxid);
        }
        if (trxid > this.oldest) {
            throw new IllegalArgumentException("oldest=" + this.oldest + " trx=" + trxid);
        }
        this.trx.put(trxid, (long)-1);
    }

    
    public static long getNewTrxId() {
        long trxId = _trxid.decrementAndGet();
        if (_log.isTraceEnabled()) {
            _log.trace("new trxid={}", trxId);
        }
        return trxId;
    }
    
    public static long getLastTrxId() {
        long trxId = _trxid.get();
        return trxId;
    }
    
    public long getCurrentVersion() {
        return _version.get();
    }
    
    public static long getNewVersion() {
        return _version.incrementAndGet();
    }

    /**
     * closed trx manager will make all pending trx rolled back
     */
    public void close() {
        this.isClosed = true;
    }

    public Map<Long, Long> getAll() {
        return Collections.unmodifiableMap(this.trx);
    }

    public int size() {
        return this.trx.size();
    }

    /**
     * release transaction older (larger) than or equal to the specified
     * 
     * @param trxid
     */
    public synchronized void freeTo(long trxid) {
        if (trxid > this.oldest) {
            return;
        }
        Iterator<Map.Entry<Long, Long>> i = this.trx.entrySet().iterator();
        int count = 0;
        for (;i.hasNext();) {
            Map.Entry<Long, Long> ii = i.next();
            if (ii.getKey() >= trxid) {
                i.remove();
                count++;
            }
        }
        if (count > 0) {
            _log.debug("{} transactions freed. transaction window reset to {} ...", count, trxid);
        }
        else {
            _log.trace("transaction window reset to {} ...", trxid);
        }
        this.oldest = trxid - 1;
    }

    public void setOldest(long oldesTrx) {
        this.oldest = oldesTrx;
    }

    public long getOldest() {
        return this.oldest;
    }
    
    public long getStartTrxId() {
        return this.oldest;
    }

    public void reset() {
        this.isClosed = false;
        this.trx.clear();
        this.oldest = -10;
    }
}
