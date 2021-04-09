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
package com.antsdb.saltedfish.sql.vdm;

import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.TrxMan;
import com.antsdb.saltedfish.sql.TableLock;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * assumption: no concurrency in transaction, meaning only one thread is accessing the methods at any time
 *  
 * @author xinyi
 *
 */
public final class Transaction {
    static Logger _log = UberUtil.getThisLogger();
    static Transaction _seeEverything = new Transaction(0, Long.MAX_VALUE);
    static Transaction _system = new Transaction(1, Long.MAX_VALUE);
    
    private volatile long trxid;
    long trxts;
    boolean isDdl = false;
    TableLock tableLock;

    /* debug code below
    @Override
    protected void finalize() throws Throwable {
        if (this.trxid < 0) {
            if (this.trxman.getTimestamp(trxid) < -10) {
                _log.error("leaked trx {}", trxid);
            }
        }
        super.finalize();
    }
    */

    public Transaction() {
    }
    
    public Transaction(long trxid, long trxts) {
        super();
        this.trxid = trxid;
        this.trxts = trxts;
    }

    public void makeAutonomous() {
        this.trxid = TrxMan.getNewVersion();
    }
    
    public long getGuaranteedTrxId() {
        if (this.trxid == 0) {
            this.trxid = TrxMan.getNewTrxId();
        }
        return this.trxid;
    }
    
    /**
     * get the transaction id. Be warned the id can be 0 because the transaction has not been realized
     * 
     *  
     * @return
     */
    public long getTrxId() {
        return this.trxid;
    }
    
    public long getTrxTs() {
        if (this.trxts == 0) {
            this.trxts = TrxMan.getNewVersion();
        }
        return this.trxts;
    }
    
    /**
     * for debug/test purpose
     * 
     * @return
     */
    public static final Transaction getSeeEverythingTrx() {
        return _seeEverything;
    }

    /**
     * there is concurrency 
     * 
     * @return
     */
    public static Transaction getSystemTransaction() {
        return _system;
    }

    public void setDdl(boolean b) {
        this.isDdl = b;
    }

    public boolean isDddl() {
        return this.isDdl;
    }

    public void releaseAllLocks() {
        // release all table locks
        if (this.tableLock != null) {
            this.tableLock.releaseAll();
        }
        this.tableLock = null;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("trxid=");
        buf.append(this.trxid);
        buf.append(",trxts=");
        buf.append(this.trxts);
        return buf.toString();
    }

    public void newTrxTs() {
        this.trxts = TrxMan.getNewVersion();
    }

    public void addLock(TableLock lock) {
        lock.makeTransactional(this.tableLock);
        this.tableLock = lock;
    }

    public void reset() {
        this.trxid = 0;
        this.trxts = 0;
        this.isDdl = false;
    }
}
