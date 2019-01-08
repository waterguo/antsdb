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
import com.antsdb.saltedfish.nosql.Gobbler.MessageEntry;
import com.antsdb.saltedfish.nosql.Gobbler.MessageEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.PutEntry;
import com.antsdb.saltedfish.nosql.Gobbler.PutEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.RowUpdateEntry;
import com.antsdb.saltedfish.nosql.Gobbler.RowUpdateEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.UpdateEntry;
import com.antsdb.saltedfish.nosql.Gobbler.UpdateEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.RollbackEntry;
import com.antsdb.saltedfish.util.UberFormatter;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author wgu0
 */
class TrxRecoverer implements ReplayHandler {
    static Logger _log = UberUtil.getThisLogger();
    
    private TrxMan trxman;
    private long trxCount;
    private long oldesTrx = Long.MIN_VALUE;

    public void run(TrxMan trxMan, Gobbler gobbler, long spStart) throws Exception {
        
        // start recovering from the start given point
        
        this.trxman = trxMan;
        _log.info("start recovering trx from {} ...", UberFormatter.hex(spStart));
        trxMan.setOldest(0);
        long end = gobbler.replay(spStart, true, this);
        _log.info("trx recovering stopped at {}", end);
        
        // ending
        
        _log.info("{} transactions have been recovered", this.trxCount);
        if (oldesTrx != Long.MIN_VALUE) {
            trxMan.setOldest(this.oldesTrx);
        }
    }

    @Override
    public void commit(CommitEntry entry) throws Exception {
        this.trxCount++;
        long trxid = entry.getTrxid();
        if (trxid >= 0) {
            return;
        }
        long version = entry.getVersion();
        this.trxman.commit(trxid, version);
        this.oldesTrx = Math.max(this.oldesTrx, trxid);
    }

    @Override
    public void rollback(RollbackEntry entry) throws Exception {
        this.trxCount++;
        long trxid = entry.getTrxid();
        if (trxid >= 0) {
            return;
        }
        this.trxman.rollback(trxid);
        this.oldesTrx = Math.max(this.oldesTrx, trxid);
    }

    @Override
    public void insert(InsertEntry entry) throws Exception {
        rowUpdate(entry);
    }

    @Override
    public void insert(InsertEntry2 entry) throws Exception {
        rowUpdate(entry);
    }

    @Override
    public void update(UpdateEntry entry) throws Exception {
        rowUpdate(entry);
    }

    @Override
    public void update(UpdateEntry2 entry) throws Exception {
        rowUpdate(entry);
    }

    @Override
    public void put(PutEntry entry) throws Exception {
        rowUpdate(entry);
    }
    
    @Override
    public void put(PutEntry2 entry) throws Exception {
        rowUpdate(entry);
    }
    
    private void rowUpdate(RowUpdateEntry entry) throws Exception {
        long pRow = entry.getRowPointer();
        long trxid = Row.getVersion(pRow);
        if (trxid >= 0) {
            return;
        }
        this.trxman.rollback(trxid);
        this.oldesTrx = Math.max(this.oldesTrx, trxid);
    }

    private void rowUpdate(RowUpdateEntry2 entry) throws Exception {
        long pRow = entry.getRowPointer();
        long trxid = Row.getVersion(pRow);
        if (trxid >= 0) {
            return;
        }
        this.trxman.rollback(trxid);
        this.oldesTrx = Math.max(this.oldesTrx, trxid);
    }

    @Override
    public void index(IndexEntry entry) throws Exception {
        long trxid = entry.getTrxid();
        if (trxid >= 0) {
            return;
        }
        this.trxman.rollback(trxid);
        this.oldesTrx = Math.max(this.oldesTrx, trxid);
    }

    @Override
    public void index(IndexEntry2 entry) throws Exception {
        long trxid = entry.getTrxid();
        if (trxid >= 0) {
            return;
        }
        this.trxman.rollback(trxid);
        this.oldesTrx = Math.max(this.oldesTrx, trxid);
    }

    @Override
    public void delete(DeleteEntry entry) throws Exception {
        long trxid = entry.getTrxid();
        if (trxid >= 0) {
            return;
        }
        this.trxman.rollback(trxid);
        this.oldesTrx = Math.max(this.oldesTrx, trxid);
    }
    
    @Override
    public void delete(DeleteEntry2 entry) throws Exception {
        long trxid = entry.getTrxid();
        if (trxid >= 0) {
            return;
        }
        this.trxman.rollback(trxid);
        this.oldesTrx = Math.max(this.oldesTrx, trxid);
    }
    
    @Override
    public void deleteRow(DeleteRowEntry entry) throws Exception {
        long trxid = entry.getTrxId();
        if (trxid >= 0) {
            return;
        }
        this.trxman.rollback(trxid);
        this.oldesTrx = Math.max(this.oldesTrx, trxid);
    }
    
    @Override
    public void deleteRow(DeleteRowEntry2 entry) throws Exception {
        long trxid = entry.getTrxId();
        if (trxid >= 0) {
            return;
        }
        this.trxman.rollback(trxid);
        this.oldesTrx = Math.max(this.oldesTrx, trxid);
    }

    @Override
    public void message(MessageEntry entry) throws Exception {
    }

    @Override
    public void message(MessageEntry2 entry) throws Exception {
    }

    @Override
    public void ddl(DdlEntry entry) throws Exception {
    }
}
