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
package com.antsdb.saltedfish.storage;

import java.io.IOException;

import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.ReplicationHandler;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.Gobbler.CommitEntry;
import com.antsdb.saltedfish.nosql.Gobbler.DeleteEntry;
import com.antsdb.saltedfish.nosql.Gobbler.DeleteRowEntry;
import com.antsdb.saltedfish.nosql.Gobbler.IndexEntry;
import com.antsdb.saltedfish.nosql.Gobbler.InsertEntry;
import com.antsdb.saltedfish.nosql.Gobbler.MessageEntry;
import com.antsdb.saltedfish.nosql.Gobbler.PutEntry;
import com.antsdb.saltedfish.nosql.Gobbler.RollbackEntry;
import com.antsdb.saltedfish.nosql.Gobbler.RowUpdateEntry;
import com.antsdb.saltedfish.nosql.Gobbler.TimestampEntry;
import com.antsdb.saltedfish.nosql.Gobbler.TransactionWindowEntry;
import com.antsdb.saltedfish.nosql.Gobbler.UpdateEntry;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class HBaseReplicationHandler extends ReplicationHandler {
    static Logger _log = UberUtil.getThisLogger();
    
    Humpback humpback;
    HBaseStorageService hbase;
    SyncBuffer buffer;
    long trxCount = 0;
    long rowCount = 0;
    long putRowCount = 0;
    long deleteRowCount = 0;
    long indexRowCount = 0;

    private long sp;
    
    public HBaseReplicationHandler(Humpback humpback, HBaseStorageService hbaseStorageService) {
        this.humpback = humpback;
        this.hbase = hbaseStorageService;
        int bufferSize = hbaseStorageService.getConfigBufferSize();
        this.buffer = new SyncBuffer(humpback, this.hbase, bufferSize);
        this.sp = this.hbase.getCurrentSP();
    }

    @Override
    public void flush() throws Exception {
        this.buffer.flush();
        long hbaseLp = this.hbase.getReplicateLogPointer();
        if (this.sp > hbaseLp) {
            this.hbase.updateLogPointer(this.sp);
        }
        else if (this.sp < hbaseLp){
            _log.error("error: lp {} is less than the one in storage {}", this.sp, hbaseLp);
        }
    }
    
    @Override
    public void insert(InsertEntry entry) throws Exception {
        rowUpdate(entry);
    }

    @Override
    public void update(UpdateEntry entry) throws Exception {
        rowUpdate(entry);
    }

    @Override
    public void put(PutEntry entry) throws Exception {
        rowUpdate(entry);
    }
    
    private void rowUpdate(RowUpdateEntry entry) throws Exception {
        int tableId = entry.getTableId();
        long pRow = entry.getRowPointer();
        Row row = Row.fromMemoryPointer(pRow, 0);
        this.buffer.addRow(tableId, row.getKeyAddress(), row.getAddress());
        this.sp = entry.getSpacePointer();
        if (this.buffer.flushIfFull()) {
            this.hbase.updateLogPointer(this.sp);
        }
    }

    @Override
    public void delete(DeleteEntry entry) throws Exception {
        int tableId = entry.getTableId();
        long pKey = entry.getKeyAddress();
        this.buffer.addDelete(tableId, pKey);
        this.sp = entry.getSpacePointer();
        if (this.buffer.flushIfFull()) {
            this.hbase.updateLogPointer(this.sp);
        }
    }


    @Override
    public void deleteRow(DeleteRowEntry entry) throws IOException {
        int tableId = entry.getTableId();
        long pRow = entry.getRowPointer();
        long pKey = Row.getKeyAddress(pRow);
        this.buffer.addDelete(tableId, pKey);
        this.sp = entry.getSpacePointer();
        if (this.buffer.flushIfFull()) {
            this.hbase.updateLogPointer(this.sp);
        }
    }
    
    @Override
    public void index(IndexEntry entry) throws Exception {
        int tableId = entry.getTableId();
        long pKey = entry.getIndexKeyAddress();
        this.buffer.addIndexLine(tableId, pKey, entry.getIndexLineAddress());
        this.sp = entry.getSpacePointer();
        if (this.buffer.flushIfFull()) {
            this.hbase.updateLogPointer(this.sp);
        }
    }

    @Override
    public void commit(CommitEntry entry) throws Exception {
        this.sp = entry.getSpacePointer();
    }

    @Override
    public void rollback(RollbackEntry entry) throws Exception {
        this.sp = entry.getSpacePointer();
    }

    @Override
    public void message(MessageEntry entry) throws Exception {
        this.sp = entry.getSpacePointer();
    }

    @Override
    public void transactionWindow(TransactionWindowEntry entry) throws Exception {
        this.sp = entry.getSpacePointer();
    }

    @Override
    public void timestamp(TimestampEntry entry) {
        this.sp = entry.getSpacePointer();
    }
}
