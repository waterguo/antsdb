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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.antsdb.saltedfish.util.UberTime;

/**
 * 
 * @author *-xguo0<@
 */
public class Statistician implements Replicable, ReplicationHandler2 {
    Humpback humpback;
    long sp;
    ConcurrentHashMap<Integer, TableStats> stats = new ConcurrentHashMap<>();
    long lastSaveTime;
    private long commitedLp;
    HumpbackSession hsession;

    public Statistician(Humpback humpback) {
        this.humpback = humpback;
        this.hsession = humpback.createSession("local/statistician");
        this.sp = this.humpback.getCheckPoint().getStatisticanLogPointer();
        if (this.sp == 0) {
            this.sp = this.humpback.getGobbler().getStartSp();
        }
        this.commitedLp = this.sp;
        load();
    }

    @Override
    public void putRow(int tableId, long pRow, long version, long pEntry, long lpEntry) throws Exception {
        LogEntry entry = LogEntry.getEntry(lpEntry, pEntry);
        if (entry instanceof InsertEntry2) {
            insert(lpEntry, tableId, pRow, version);
        }
        else if (entry instanceof UpdateEntry2) {
            update(lpEntry, tableId, pRow, version);
        }
        else if (entry instanceof PutEntry2) {
            put(lpEntry, tableId, pRow, version);
        }
        else {
            throw new IllegalArgumentException();
        }
    }

    private void insert(long sp, int tableId, long pRow, long version) throws Exception {
        Row row = Row.fromMemoryPointer(pRow, version);
        if (tableId == Humpback.SYSSTATS_TABLE_ID) {
            return;
        }
        TableStats table = getTableStats(tableId);
        table.inspectInsert(row, sp);
        this.sp = sp;
    }

    private void update(long sp, int tableId, long pRow, long version) throws Exception {
        Row row = Row.fromMemoryPointer(pRow, version);
        if (tableId == Humpback.SYSSTATS_TABLE_ID) {
            return;
        }
        TableStats table = getTableStats(tableId);
        table.inspectUpdate(row, sp);
        this.sp = sp;
    }

    private void put(long sp, int tableId, long pRow, long version) throws Exception {
        Row row = Row.fromMemoryPointer(pRow, version);
        if (tableId == Humpback.SYSSTATS_TABLE_ID) {
            return;
        }
        TableStats table = getTableStats(tableId);
        table.inspectPut(row, sp);
        this.sp = sp;
    }

    @Override
    public void putIndex(int tableId, long pIndexKey, long pIndex, long version, long pEntry, long lpEntry)
    throws Exception {
        this.sp = lpEntry;
    }

    @Override
    public void deleteIndex(int tableId, long pKey, long version, long pEntry, long lpEntry) throws Exception {
        if (tableId == Humpback.SYSSTATS_TABLE_ID) {
            return;
        }
        TableStats table = getTableStats(tableId);
        table.inspectDelete(lpEntry);
        this.sp = lpEntry;
    }
    
    @Override
    public void deleteRow(int tableId, long pKey, long version, long pEntry, long lpEntry) throws Exception {
        if (tableId == Humpback.SYSSTATS_TABLE_ID) {
            return;
        }
        TableStats table = getTableStats(tableId);
        table.inspectDelete(lpEntry);
        this.sp = lpEntry;
    }

    @Override
    public long getReplicateLogPointer() {
        return this.sp;
    }

    @Override
    public ReplicationHandler2 getReplayHandler() {
        return this;
    }

    @Override
    public void flush(long lpRows, long lpIndexes) throws Exception {
        if ((UberTime.getTime() - this.lastSaveTime) >= 30 * 1000) {
            save();
        }
    }

    private TableStats getTableStats(int tableId) {
        TableStats result = this.stats.get(tableId);
        if (result == null) {
            result = new TableStats();
            result.serverId = this.humpback.getServerId();
            result.tableId = tableId;
            this.stats.putIfAbsent(tableId, result);
            result = this.stats.get(tableId);
        }
        return result;
    }

    public Map<Integer,TableStats> getStats() {
        return this.stats;
    }
    
    void save() {
        try (HumpbackSession foo = this.hsession.open()) {
            GTable gtable = this.humpback.getTable(Humpback.SYSSTATS_TABLE_ID);
            for (TableStats i:this.stats.values()) {
                if (i.isDirty) {
                    SlowRow row = i.save();
                    gtable.put(hsession, 1, row, 0);
                }
            }
            this.lastSaveTime = UberTime.getTime();
            humpback.getCheckPoint().setStatisticanLogPointer(this.sp);
            this.commitedLp = this.sp;
        }
    }

    void load() {
        this.stats.clear();
        this.sp = this.humpback.getGobbler().getStartSp();
        GTable gtable = this.humpback.getTable(Humpback.SYSSTATS_TABLE_ID);
        for (RowIterator i=gtable.scan(1, 1, true); i.next();) {
            Row row = i.getRow();
            TableStats tableStats = new TableStats();
            tableStats.load(row);
            this.stats.put(tableStats.tableId, tableStats);
            if (tableStats.serverId == this.humpback.getServerId()) {
                this.sp = Math.max(this.sp, tableStats.sp);
            }
        }
    }

    @Override
    public void transactionWindow(long pEntry, long lpEntry) throws Exception {
        this.sp = lpEntry;
    }

    @Override
    public long getCommittedLogPointer() {
        return this.commitedLp;
    }

    @Override
    public void message(long pEntry, long lpEntry) throws Exception {
        this.sp = lpEntry;
    }


    @Override
    public void ddl(long pEntry, long lpEntry) throws Exception {
        this.sp = lpEntry;
    }
}
