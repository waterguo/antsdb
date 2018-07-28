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
import com.antsdb.saltedfish.nosql.Gobbler.DeleteRowEntry;
import com.antsdb.saltedfish.nosql.Gobbler.InsertEntry;
import com.antsdb.saltedfish.nosql.Gobbler.PutEntry;
import com.antsdb.saltedfish.nosql.Gobbler.RollbackEntry;
import com.antsdb.saltedfish.nosql.Gobbler.TransactionWindowEntry;
import com.antsdb.saltedfish.nosql.Gobbler.UpdateEntry;
import com.antsdb.saltedfish.util.JumpException;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
class TransactionScanner implements ReplayHandler {
    private static final Logger _log = UberUtil.getThisLogger();
    
    private TrxMan trxman;
    private long stopId;
    private long lp;

    TransactionScanner(TrxMan trxman, long lp) {
        this.trxman = trxman;
        this.lp = lp;
    }
    
    @Override
    public void insert(InsertEntry entry) throws Exception {
        this.lp = entry.getSpacePointer();
    }

    @Override
    public void update(UpdateEntry entry) throws Exception {
        this.lp = entry.getSpacePointer();
    }

    @Override
    public void put(PutEntry entry) throws Exception {
        this.lp = entry.getSpacePointer();
    }

    @Override
    public void deleteRow(DeleteRowEntry entry) throws Exception {
        this.lp = entry.getSpacePointer();
    }

    @Override
    public void commit(CommitEntry entry) throws Exception {
        long trxid = entry.getTrxid();
        this.trxman.commit(trxid, entry.getVersion());
        this.lp = entry.getSpacePointer();
        if (trxid == this.stopId) {
            throw new JumpException();
        }
    }

    @Override
    public void rollback(RollbackEntry entry) throws Exception {
        long trxid = entry.getTrxid();
        this.trxman.rollback(trxid);
        this.lp = entry.getSpacePointer();
        if (trxid == this.stopId) {
            throw new JumpException();
        }
    }

    @Override
    public void transactionWindow(TransactionWindowEntry entry) throws Exception {
        if (entry.getTrxid() <= this.stopId) {
            throw new JumpException();
        }
    }

    public void scan(Gobbler gobbler, long trxid) {
        try {
            this.stopId = trxid;
            gobbler.replay(this.lp, false, this);
        }
        catch (JumpException x) {
        }
        catch (Exception x) {
            _log.error("error", x);
        }
    }

}
