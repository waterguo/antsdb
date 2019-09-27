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

import com.antsdb.saltedfish.cpp.KeyBytes;
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
import com.antsdb.saltedfish.nosql.Gobbler.RollbackEntry;
import com.antsdb.saltedfish.nosql.Gobbler.RowUpdateEntry;
import com.antsdb.saltedfish.nosql.Gobbler.RowUpdateEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.TransactionWindowEntry;
import com.antsdb.saltedfish.nosql.Gobbler.UpdateEntry;
import com.antsdb.saltedfish.nosql.Gobbler.UpdateEntry2;
import com.antsdb.saltedfish.util.LongLong;
import com.antsdb.saltedfish.util.UberUtil;
import static com.antsdb.saltedfish.util.UberFormatter.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * recover the database from corruption
 * 
 * @author wgu0
 */
class Recoverer implements ReplayHandler {
    static Logger _log = UberUtil.getThisLogger();

    Humpback humpback;
    Gobbler gobbler;
    long trxCount = 0;
    long rowCount = 0;
    TrxMan trxman;
    Set<Integer> tablesDeleted = new HashSet<>();

    public Recoverer(Humpback humpback, Gobbler gobbler) {
        super();
        this.humpback = humpback;
        this.gobbler = gobbler;
        this.trxman = humpback.getTrxMan();
        for (SysMetaRow i:humpback.getTablesMeta()) {
            if (i.isDeleted()) {
                this.tablesDeleted.add(i.getTableId());
            }
        }
    }

    public void run() throws Exception {
        boolean inclusive;
        long start;
        LongLong span = this.humpback.getStorageEngine().getLogSpan();
        if (span != null) {
            start = span.y;
            if (start < gobbler.getStartSp()) {
                start = gobbler.getStartSp();
            }
            inclusive = false;
        }
        else {
            start = this.gobbler.getStartSp();
            inclusive = true;
        }
        _log.info("start recovering from {} to {}", hex(start), hex(this.humpback.spaceman.getAllocationPointer()));
        this.gobbler.replay(start, inclusive, this);

        // ending

        _log.info("{} rows have been recovered", this.rowCount);
        _log.info("{} transactions have been recovered", this.trxCount);
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
        rowUpdate(entry.getSpacePointer(), 
                entry.getTrxId(), 
                entry.getTableId(), 
                entry.getRowSpacePointer(), 
                entry.getRowPointer());
    }
    
    private void rowUpdate(RowUpdateEntry2 entry) throws Exception {
        rowUpdate(entry.getSpacePointer(), 
                  entry.getTrxId(), 
                  entry.getTableId(), 
                  entry.getRowSpacePointer(), 
                  entry.getRowPointer());
    }
    
    private void rowUpdate(long sp, long trxid, int tableId, long spRow, long pRow) throws Exception {
        if (tableId < 0) {
            // dont recover temporary table
            return;
        }
        long version = Row.getVersion(pRow);
        if (this.tablesDeleted.contains(tableId)) {
            return;
        }
        GTable table = this.humpback.getTable(tableId);
        long pKey = Row.getKeyAddress(pRow);
        if (_log.isTraceEnabled()) {
            _log.trace("put @ {} tableId={} version={} key={}", sp, tableId, version, KeyBytes.create(pKey).toString());
        }
        if (table == null) {
            _log.warn("unable to recover row @ {}. table {} not found", sp, tableId);
            return;
        }
        LongLong span = table.memtable.getLogSpan();
        if ((span != null) && (sp <= span.y)) {
            // space pointer i ahead of end row space pointer means the
            // operation has already applied
            _log.trace("put @ {} is ignored", sp);
            return;
        }
        HumpbackError error = table.memtable.recover(sp);
        if (HumpbackError.SUCCESS != error) {
            _log.warn("unable to recover row @ {} due to {}", sp, error);
            _log.warn("trxid = {}", trxid);
            _log.warn("table = {}", tableId);
            Row row = Row.fromMemoryPointer(pRow, version);
            _log.warn("rowkey = {}", KeyBytes.toString(row.getKeyAddress()));
            _log.warn("rowid = {}", row.get(0));
            return;
        }

        syncSchema(sp, pRow, tableId);
        countRowUpdates();
    }

    @Override
    public void delete(DeleteEntry entry) {
        delete(entry.getSpacePointer(), entry.getTrxid(), entry.getTableId(), entry.getKeyAddress());
    }
    
    @Override
    public void delete(DeleteEntry2 entry) {
        delete(entry.getSpacePointer(), entry.getTrxid(), entry.getTableId(), entry.getKeyAddress());
    }
    
    @Override
    public void deleteRow(DeleteRowEntry entry) throws Exception {
        long pRow = entry.getRowPointer();
        long pKey = Row.getKeyAddress(pRow);
        delete(entry.getSpacePointer(), entry.getTrxId(), entry.getTableId(), pKey);
    }

    @Override
    public void deleteRow(DeleteRowEntry2 entry) throws Exception {
        long pRow = entry.getRowPointer();
        long pKey = Row.getKeyAddress(pRow);
        delete(entry.getSpacePointer(), entry.getTrxId(), entry.getTableId(), pKey);
    }

    private void delete(long sp, long trxid, int tableId, long pKey) {
        if (tableId < 0) {
            // dont recover temporary table
            return;
        }
        if (_log.isTraceEnabled()) {
            _log.trace("delete @ {} tableId={} version={} key={}", sp, tableId, trxid,
                    KeyBytes.create(pKey).toString());
        }
        GTable table = this.humpback.getTable(tableId);
        if (table == null) {
            _log.warn("unable to recover row @ {}. table {} not found", sp, tableId);
            return;
        }
        LongLong span = table.memtable.getLogSpan();
        if ((span != null) && (sp <= span.y)) {
            // space pointer i ahead of end row space pointer means the
            // operation has already applied
            _log.trace("delete @ {} is ignored", sp);
            return;
        }
        HumpbackError error = table.memtable.recover(sp);
        if (HumpbackError.SUCCESS != error) {
            _log.warn("unable to recover row @ {} due to {}", sp, error);
            return;
        }
        syncSchema(sp, 0, tableId);
        countRowUpdates();
    }

    @Override
    public void commit(CommitEntry entry) {
        long sp = entry.getSpacePointer();
        long trxid = entry.getTrxid();
        long trxts = entry.getVersion();

        _log.trace("commit @ {} trxid={} trxts={}", sp, trxid, trxts);
        this.trxman.commit(trxid, trxts);
        this.trxCount++;
    }

    @Override
    public void rollback(RollbackEntry entry) {
        long sp = entry.getSpacePointer();
        long trxid = entry.getTrxid();

        _log.trace("commit @ {} trxid={}", sp, trxid);
        this.trxman.rollback(trxid);
        this.trxCount++;
    }

    @Override
    public void index(IndexEntry entry) {
        index(entry.getSpacePointer(), 
              entry.getTrxid(), 
              entry.getTableId(), 
              entry.getIndexKeyAddress(), 
              entry.getRowKeyAddress(), 
              entry.getMisc());  
    }
    
    @Override
    public void index(IndexEntry2 entry) {
        index(entry.getSpacePointer(), 
              entry.getTrxid(), 
              entry.getTableId(), 
              entry.getIndexKeyAddress(), 
              entry.getRowKeyAddress(), 
              entry.getMisc());  
    }
    
    private void index(long sp, long trxid, int tableId, long pIndexKey, long pRowKey, byte misc) {
        if (tableId < 0) {
            // dont recover temporary table
            return;
        }
        if (_log.isTraceEnabled()) {
            _log.trace("index @ {} tableId={} version={} key={}", 
                       sp, 
                       tableId, 
                       trxid, 
                       KeyBytes.create(pIndexKey).toString());
        }
        GTable table = this.humpback.getTable(tableId);
        if (table == null) {
            _log.warn("unable to recover index @ {}. table {} not found", sp, tableId);
            return;
        }
        LongLong span = table.memtable.getLogSpan();
        if ((span != null) && (sp <= span.y)) {
            // space pointer i ahead of end row space pointer means the
            // operation has already applied
            _log.trace("index @ {} is ignored", sp);
            return;
        }
        HumpbackError error = table.memtable.recover(sp);
        if (HumpbackError.SUCCESS != error) {
            _log.warn("unable to recover index @ {} due to {}", sp, error);
            return;
        }
        countRowUpdates();
    }

    @Override
    public void transactionWindow(TransactionWindowEntry entry) throws Exception {
        long oldestTrxId = entry.getTrxid();
        render(oldestTrxId);
        this.humpback.trxMan.freeTo(oldestTrxId + 100);
        _log.info("recovery progress: {}", hex(entry.getSpacePointer()));
    }

    @Override
    public void message(MessageEntry entry) throws Exception {
    }

    @Override
    public void message(MessageEntry2 entry) throws Exception {
    }

    private void render(long trxid) throws IOException {
        for (GTable table:this.humpback.getTables()) {
            table.memtable.render(trxid);
            table.memtable.carbonfreezeIfPossible(trxid);
        }
    }

    private void countRowUpdates() {
        this.rowCount++;
    }
    
    private void syncSchema(long sp, long pRow, int tableId) {
        // if system table is touched, synchronize with file system
        if (tableId == Humpback.SYSMETA_TABLE_ID) {
            if (pRow != 0) {
                SysMetaRow row = new SysMetaRow(SlowRow.fromRowPointer(pRow, 0));
                if (row.getTableId() < 0) {
                    // do nothing if the schema change is caused by temp. table
                    return;
                }
            }
            try {
                humpback.recoverTables();
            }
            catch (Exception e) {
                _log.warn("unable to recreate table {} @ {}.", sp, e);
                return;
            }
        }
    }

    @Override
    public void ddl(DdlEntry entry) throws Exception {
    }

}
