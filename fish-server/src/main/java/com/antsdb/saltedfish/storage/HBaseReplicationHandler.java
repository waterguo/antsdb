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
import com.antsdb.saltedfish.nosql.Replicable;
import com.antsdb.saltedfish.nosql.ReplicationHandler2;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.util.UberFormatter;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class HBaseReplicationHandler implements Replicable, ReplicationHandler2 {
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
    public void setLogPointer(long value) {
        this.hbase.cp.setLogPointer(value);
    }

    @Override
    public void connect() throws IOException {
        this.buffer.connect();
    }
    
    @Override
    public long getReplicateLogPointer() {
        return this.hbase.cp.getCurrentSp();
    }

    @Override
    public ReplicationHandler2 getReplayHandler() {
        return this;
    }

    @Override
    public long getCommittedLogPointer() {
        return this.hbase.cp.getCurrentSp();
    }

    
    @Override
    public void flush(long lpRows, long lpIndexes) throws Exception {
        this.buffer.flush();
        long hbaseLp = this.hbase.cp.getCurrentSp();
        if (this.sp > hbaseLp) {
            this.hbase.updateLogPointer(this.sp);
        }
        else if (this.sp < hbaseLp){
            _log.error("error: lp {} is less than the one in storage {}", this.sp, hbaseLp);
        }
    }
    
    @Override
    public void putRow(int tableId, long pRow, long version, long pEntry, long lpEntry) throws Exception {
        Row row = Row.fromMemoryPointer(pRow, version);
        this.buffer.addRow(tableId, row.getKeyAddress(), lpEntry, version);
        this.sp = lpEntry;
        flushIfFull(tableId);
    }

    @Override
    public void deleteRow(int tableId, long pKey, long version, long pEntry, long lpEntry) throws Exception {
        this.buffer.addDelete(tableId, pKey, lpEntry, version);
        this.sp = lpEntry;
        flushIfFull(tableId);
    }
    
    @Override
    public void deleteIndex(int tableId, long pKey, long version, long pEntry, long lpEntry) throws Exception {
        this.buffer.addDelete(tableId, pKey, lpEntry, version);
        this.sp = lpEntry;
        flushIfFull(tableId);
    }

    @Override
    public void putIndex(int tableId, long pIndexKey, long pIndex, long version, long pEntry, long lpEntry)
    throws Exception {
        this.buffer.addIndexLine(tableId, pIndexKey, lpEntry, version);
        this.sp = lpEntry;
        flushIfFull(tableId);
    }

    @Override
    public void commit(long pEntry, long lpEntry) throws Exception {
        this.sp = lpEntry;
    }

    @Override
    public void rollback(long pEntry, long lpEntry) throws Exception {
        this.sp = lpEntry;
    }

    @Override
    public void message(long pEntry, long lpEntry) throws Exception {
        this.sp = lpEntry;
    }

    @Override
    public void transactionWindow(long pEntry, long lpEntry) throws Exception {
        this.sp = lpEntry;
    }

    @Override
    public void timestamp(long pEntry, long lpEntry) {
        this.sp = lpEntry;
    }

    @Override
    public void ddl(long pEntry, long lpEntry) {
    };
    
    private void flushIfFull(int tableId) throws IOException {
        if (this.buffer.flushIfFull(tableId)) {
            this.hbase.updateLogPointer(this.sp);
            _log.trace("hbase checkpoint is updated with lp={}", UberFormatter.hex(this.sp));
        }
    }
}
