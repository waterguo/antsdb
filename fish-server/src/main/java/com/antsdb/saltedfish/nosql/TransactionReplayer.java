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
 * filter out rolled back data
 * 
 * @author *-xguo0<@
 */
public class TransactionReplayer implements ReplayHandler {
    private static Logger _log = UberUtil.getThisLogger();
    
    private ReplicationHandler2 downstream;
    private long sp;
    private TrxMan trxman;
    private TransactionScanner trxScanner;
    private Gobbler gobbler;
    private long lpLastTrxWindow;
    
    public TransactionReplayer(Gobbler gobbler, ReplicationHandler2 downstream) {
        this.gobbler = gobbler;
        this.trxman = new TrxMan(null);
        this.trxman.close();
        this.downstream = downstream;
    }
    
    @Override
    public void insert(InsertEntry2 entry) throws Exception {
        putRow(entry);
    }

    @Override
    public void update(UpdateEntry2 entry) throws Exception {
        putRow(entry);
    }

    @Override
    public void put(PutEntry2 entry) throws Exception {
        putRow(entry);
    }

    private void putRow(RowUpdateEntry2 entry) throws Exception {
        long version = getTrxVersion(entry.getSpacePointer(), entry.getTrxId());
        if (version == TrxMan.MARK_ROLLED_BACK) {
            return;
        }
        int tableId = entry.getTableId();
        this.downstream.putRow(tableId, entry.getRowPointer(), version, entry.getAddress(), entry.getSpacePointer());
    }
    
    @Override
    public void index(IndexEntry2 entry) throws Exception {
        long version = getTrxVersion(entry.getSpacePointer(), entry.getTrxid());
        if (version == TrxMan.MARK_ROLLED_BACK) {
            return;
        }
        long pIndexKey = entry.getIndexKeyAddress();
        long pIndex = entry.getRowKeyAddress();
        int tableId = entry.getTableId();
        this.downstream.putIndex(tableId, pIndexKey, pIndex, version, entry.getAddress(), entry.getSpacePointer());
    }

    @Override
    public void deleteRow(DeleteRowEntry2 entry) throws Exception {
        long version = getTrxVersion(entry.getSpacePointer(), entry.getTrxId());
        if (version == TrxMan.MARK_ROLLED_BACK) {
            return;
        }
        int tableId = entry.getTableId();
        long pRow = entry.getRowPointer();
        Row row = Row.fromMemoryPointer(pRow, version);
        this.downstream.deleteRow(tableId, row.getKeyAddress(), version, entry.getAddress(), entry.getSpacePointer());
    }

    @Override
    public void delete(DeleteEntry2 entry) throws Exception {
        long version = getTrxVersion(entry.getSpacePointer(), entry.getTrxid());
        if (version == TrxMan.MARK_ROLLED_BACK) {
            return;
        }
        int tableId = entry.getTableId();
        long pKey = entry.getKeyAddress();
        this.downstream.deleteIndex(tableId, pKey, version, entry.getAddress(), entry.getSpacePointer());
    }

    @Override
    public void all(LogEntry entry) throws Exception {
        this.downstream.all(entry.getAddress(), entry.getSpacePointer());
    }

    @Override
    public void commit(CommitEntry entry) throws Exception {
        this.downstream.commit(entry.getAddress(), entry.getSpacePointer());
    }

    @Override
    public void rollback(RollbackEntry entry) throws Exception {
        this.downstream.rollback(entry.getAddress(), entry.getSpacePointer());
    }

    @Override
    public void message(MessageEntry entry) throws Exception {
        this.downstream.message(entry.getAddress(), entry.getSpacePointer());
    }

    @Override
    public void message(MessageEntry2 entry) throws Exception {
        this.downstream.message(entry.getAddress(), entry.getSpacePointer());
    }

    @Override
    public void transactionWindow(TransactionWindowEntry entry) throws Exception {
        this.downstream.transactionWindow(entry.getAddress(), entry.getSpacePointer());
        _log.debug("trx window @ {}", Long.toHexString(entry.getSpacePointer()));
        this.trxman.freeTo(entry.getTrxid());
        this.lpLastTrxWindow = entry.sp;
    }

    @Override
    public void ddl(DdlEntry entry) throws Exception {
        this.downstream.ddl(entry.getAddress(), entry.getSpacePointer());
    }
    
    private long getTrxVersion(long lp, long trxid) {
        for (int i=0; i<2; i++) {
            long version = trxman.getTimestamp(trxid);
            if (version >= 0) {
                return version;
            }
            if (version == TrxMan.MARK_ROLLED_BACK) {
                return version;
            }
            if (this.trxScanner == null) {
                this.trxScanner = new TransactionScanner(this.gobbler, this.trxman, lp);
            }
            else if (lp < this.lpLastTrxWindow) {
                // last replay wasnt total success. this replay is before the last trx window position. 
                // we might have lost some useful transactions. reset the scanner so we can recover
                _log.warn("lp {} is smaller than last trx window {}", lp, this.lpLastTrxWindow);
                this.trxScanner = new TransactionScanner(this.gobbler, this.trxman, lp);
            }
            this.trxScanner.scan(this.gobbler, trxid);
        }
        throw new InvalidTransactionIdException(trxid);
    }

    public static long replay(Humpback humpback, long spStart, boolean inclusive, ReplicationHandler2 handler) 
    throws Exception {
        Gobbler gobbler = humpback.getGobbler();
        if (gobbler == null) {
            // gobbler could be null in the initialization stage when humpback is not ready
            return spStart;
        }
        TransactionReplayer replayer = new TransactionReplayer(humpback.getGobbler(), handler);
        replayer.sp = spStart;
        try {
            SequentialLogReader reader = new SequentialLogReader(humpback.getGobbler());
            reader.setPosition(spStart, inclusive);
            return reader.replay(replayer);
        }
        catch (JumpException x) {
            return replayer.sp;
        }
    }

    public void resetTransactionWindow() {
        this.trxman.setOldest(-10);
    }

}
