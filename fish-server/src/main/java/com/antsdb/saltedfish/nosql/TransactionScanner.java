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

import com.antsdb.saltedfish.util.JumpException;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
class TransactionScanner {
    private static final Logger _log = UberUtil.getThisLogger();
    
    private TrxMan trxman;
    private long stopId;
    private SequentialLogReader reader;

    TransactionScanner(Gobbler gobbler, TrxMan trxman, long lp) {
        this.trxman = trxman;
        this.reader = new SequentialLogReader(gobbler);
        this.reader.setPosition(lp, false);
    }
    
    void commit(CommitEntry entry) throws Exception {
        long trxid = entry.getTrxid();
        try {
            this.trxman.commit(trxid, entry.getVersion());
        }
        catch (IllegalArgumentException ignored) {
            // there is a chance TransactionScanner is falling behind TransactionReplayer. See TestTransactionReplayer
        }
        if (trxid == this.stopId) {
            throw new JumpException();
        }
    }

    void rollback(RollbackEntry entry) throws Exception {
        long trxid = entry.getTrxid();
        this.trxman.rollback(trxid);
        if (trxid == this.stopId) {
            throw new JumpException();
        }
    }

    public void transactionWindow(TransactionWindowEntry entry) throws Exception {
        this.trxman.setOldest(entry.getTrxid());
        if (entry.getTrxid() <= this.stopId) {
            throw new JumpException();
        }
    }

    public void scan(Gobbler gobbler, long trxid) {
        try {
            this.stopId = trxid;
            for (;;) {
                LogEntry i = this.reader.read();
                if (i == null) break;
                if (i instanceof CommitEntry) {
                    commit((CommitEntry) i);
                }
                else if (i instanceof RollbackEntry) {
                    rollback((RollbackEntry) i);
                }
                else if (i instanceof TransactionWindowEntry) {
                    transactionWindow((TransactionWindowEntry) i);
                }
            }
        }
        catch (JumpException x) {
        }
        catch (Exception x) {
            _log.error("error", x);
        }
    }
}
