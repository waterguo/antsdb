/*-------------------------------------------------------------------------------------------------
 _______ __   _ _______ _______ ______  ______
 |_____| | \  |    |    |______ |     \ |_____]
 |     | |  \_|    |    ______| |_____/ |_____]

 Copyright (c) 2016, antsdb.com and/or its affiliates. All rights reserved. *-xguo0<@

 This program is free software: you can redistribute it and/or modify it under the terms of the
 GNU Affero General Public License, version 3, as published by the Free Software Foundation.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/agpl-3.0.txt>
-------------------------------------------------------------------------------------------------*/
package com.antsdb.saltedfish.nosql;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.CodingError;
import com.antsdb.saltedfish.util.IdGenerator;
import com.antsdb.saltedfish.util.UberUtil;

public final class TrxMan {
	static Logger _log = UberUtil.getThisLogger();

	public final static int MARK_ROLLED_BACK = -1;
	final static int MARK_ROW_LOCK = -3;
	
    Map<Long, Long> trx = new ConcurrentHashMap<Long, Long>(1000);
    AtomicLong trxid = new AtomicLong(IdGenerator.getId());
    AtomicLong version = new AtomicLong(trxid.get());
	private boolean isClosed;
	private long oldest = -10;
    private SpaceManager sm; 
    
	public TrxMan(SpaceManager sm) {
	    this.sm = sm;
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
    	if (trx > this.oldest) {
    		throw new IllegalArgumentException("oldest=" + this.oldest + " trx=" + trx);
    	}
        Long timestamp = this.trx.get(trx);
        if (timestamp != null) {
        	return timestamp;
        }
        return this.isClosed ? MARK_ROLLED_BACK : trx;
    }

    public void commit(long trxid, long trxts) {
    	if (_log.isTraceEnabled()) {
    		_log.trace("commit trxid={} trxts={}", trxid, trxts);
    	}
    	if (trxid > this.oldest) {
    		throw new IllegalArgumentException("oldest=" + this.oldest + " trx=" + trxid);
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

    public long getCurrentSp() {
        return this.sm.getAllocationPointer();
    }
    
    public long getNewTrxId() {
    	if (this.isClosed) {
    		throw new CodingError();
    	}
    	long trxId = -this.trxid.incrementAndGet();
    	if (_log.isTraceEnabled()) {
    		_log.trace("new trxid={}", trxId);
    	}
    	return trxId;
    }
    
    public long getLastTrxId() {
    	long trxId = -this.trxid.get();
    	return trxId;
    }

    public long getCurrentVersion() {
    	return this.version.get();
    }
    
	public long getNewVersion() {
		return this.version.incrementAndGet();
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
	 * release transaction older (larger) than the specified
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
