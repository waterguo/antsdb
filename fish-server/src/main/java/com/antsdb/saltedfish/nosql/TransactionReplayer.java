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

import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.Gobbler.CommitEntry;
import com.antsdb.saltedfish.nosql.Gobbler.DdlEntry;
import com.antsdb.saltedfish.nosql.Gobbler.DeleteEntry;
import com.antsdb.saltedfish.nosql.Gobbler.DeleteEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.DeleteRowEntry;
import com.antsdb.saltedfish.nosql.Gobbler.DeleteRowEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.IndexEntry;
import com.antsdb.saltedfish.nosql.Gobbler.IndexEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.InsertEntry;
import com.antsdb.saltedfish.nosql.Gobbler.InsertEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.LogEntry;
import com.antsdb.saltedfish.nosql.Gobbler.MessageEntry;
import com.antsdb.saltedfish.nosql.Gobbler.MessageEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.PutEntry;
import com.antsdb.saltedfish.nosql.Gobbler.PutEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.RollbackEntry;
import com.antsdb.saltedfish.nosql.Gobbler.TransactionWindowEntry;
import com.antsdb.saltedfish.nosql.Gobbler.UpdateEntry;
import com.antsdb.saltedfish.nosql.Gobbler.UpdateEntry2;
import com.antsdb.saltedfish.util.JumpException;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * filter out rolled back data
 * 
 * @author *-xguo0<@
 */
public class TransactionReplayer implements ReplayHandler {
    private static Logger _log = UberUtil.getThisLogger();
    
    private ReplayHandler downstream;
    private long sp;
    private TrxMan trxman;
    private TransactionScanner trxScanner;
    private Gobbler gobbler;
    private long lpLastTrxWindow;
    
    public TransactionReplayer(Gobbler gobbler, ReplayHandler downstream) {
        this.gobbler = gobbler;
        this.trxman = new TrxMan(null);
        this.downstream = downstream;
    }
    
    @Override
    public void insert(InsertEntry entry) throws Exception {
        if (isTrxRolledBack(entry.getSpacePointer(), entry.getTrxId())) {
            return;
        }
        this.downstream.insert(entry);
    }

    @Override
    public void insert(InsertEntry2 entry) throws Exception {
        if (isTrxRolledBack(entry.getSpacePointer(), entry.getTrxId())) {
            return;
        }
        this.downstream.insert(entry);
    }

    @Override
    public void update(UpdateEntry entry) throws Exception {
        if (isTrxRolledBack(entry.getSpacePointer(), entry.getTrxId())) {
            return;
        }
        this.downstream.update(entry);
    }

    @Override
    public void update(UpdateEntry2 entry) throws Exception {
        if (isTrxRolledBack(entry.getSpacePointer(), entry.getTrxId())) {
            return;
        }
        this.downstream.update(entry);
    }

    @Override
    public void put(PutEntry entry) throws Exception {
        if (isTrxRolledBack(entry.getSpacePointer(), entry.getTrxId())) {
            return;
        }
        this.downstream.put(entry);
    }

    @Override
    public void put(PutEntry2 entry) throws Exception {
        if (isTrxRolledBack(entry.getSpacePointer(), entry.getTrxId())) {
            return;
        }
        this.downstream.put(entry);
    }

    @Override
    public void index(IndexEntry entry) throws Exception {
        if (isTrxRolledBack(entry.getSpacePointer(), entry.getTrxid())) {
            return;
        }
        this.downstream.index(entry);
    }

    @Override
    public void index(IndexEntry2 entry) throws Exception {
        if (isTrxRolledBack(entry.getSpacePointer(), entry.getTrxid())) {
            return;
        }
        this.downstream.index(entry);
    }

    @Override
    public void deleteRow(DeleteRowEntry entry) throws Exception {
        if (isTrxRolledBack(entry.getSpacePointer(), entry.getTrxId())) {
            return;
        }
        this.downstream.deleteRow(entry);
    }

    @Override
    public void deleteRow(DeleteRowEntry2 entry) throws Exception {
        if (isTrxRolledBack(entry.getSpacePointer(), entry.getTrxId())) {
            return;
        }
        this.downstream.deleteRow(entry);
    }

    @Override
    public void delete(DeleteEntry entry) throws Exception {
        if (isTrxRolledBack(entry.getSpacePointer(), entry.getTrxid())) {
            return;
        }
        this.downstream.delete(entry);
    }

    @Override
    public void delete(DeleteEntry2 entry) throws Exception {
        if (isTrxRolledBack(entry.getSpacePointer(), entry.getTrxid())) {
            return;
        }
        this.downstream.delete(entry);
    }

    @Override
    public void all(LogEntry entry) throws Exception {
        this.downstream.all(entry);
    }

    @Override
    public void commit(CommitEntry entry) throws Exception {
        this.downstream.commit(entry);
    }

    @Override
    public void rollback(RollbackEntry entry) throws Exception {
        this.downstream.rollback(entry);
    }

    @Override
    public void message(MessageEntry entry) throws Exception {
        this.downstream.message(entry);
    }

    @Override
    public void message(MessageEntry2 entry) throws Exception {
        this.downstream.message(entry);
    }

    @Override
    public void transactionWindow(TransactionWindowEntry entry) throws Exception {
        this.downstream.transactionWindow(entry);
        _log.debug("trx window @ {}", Long.toHexString(entry.getSpacePointer()));
        this.trxman.freeTo(entry.getTrxid());
        this.lpLastTrxWindow = entry.sp;
    }

    private boolean isTrxRolledBack(long lp, long trxid) {
        for (int i=0; i<2; i++) {
            long version = trxman.getTimestamp(trxid);
            if (version >= 0) {
                return false;
            }
            if (version == TrxMan.MARK_ROLLED_BACK) {
                return true;
            }
            if (this.trxScanner == null) {
                this.trxScanner = new TransactionScanner(this.trxman, lp);
            }
            else if (lp < this.lpLastTrxWindow) {
                // last replay wasnt total success. this replay is before the last trx window position. 
                // we might have lost some useful transactions. reset the scanner so we can recover
                _log.warn("lp {} is smaller than last trx window {}", lp, this.lpLastTrxWindow);
                this.trxScanner = new TransactionScanner(this.trxman, lp);
            }
            this.trxScanner.scan(this.gobbler, trxid);
        }
        throw new InvalidTransactionIdException(trxid);
    }

    public static long replay(Humpback humpback, long spStart, boolean inclusive, ReplayHandler handler) 
    throws Exception {
        Gobbler gobbler = humpback.getGobbler();
        if (gobbler == null) {
            // gobbler could be null in the initialization stage when humpback is not ready
            return spStart;
        }
        TransactionReplayer replayer = new TransactionReplayer(humpback.getGobbler(), handler);
        replayer.sp = spStart;
        try {
            return humpback.getGobbler().replay(spStart, inclusive, replayer);
        }
        catch (JumpException x) {
            return replayer.sp;
        }
    }

    public void resetTransactionWindow() {
        this.trxman.setOldest(-10);
    }

    @Override
    public void ddl(DdlEntry entry) throws Exception {
        this.downstream.ddl(entry);
    }
}
