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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.NotImplementedException;

import com.antsdb.saltedfish.nosql.Gobbler.DeleteEntry;
import com.antsdb.saltedfish.nosql.Gobbler.DeleteRowEntry;
import com.antsdb.saltedfish.nosql.Gobbler.IndexEntry;
import com.antsdb.saltedfish.nosql.Gobbler.InsertEntry;
import com.antsdb.saltedfish.nosql.Gobbler.PutEntry;
import com.antsdb.saltedfish.nosql.Gobbler.TransactionWindowEntry;
import com.antsdb.saltedfish.nosql.Gobbler.UpdateEntry;
import com.antsdb.saltedfish.util.UberTime;

/**
 * 
 * @author *-xguo0<@
 */
public class Statistician extends ReplicationHandler implements Replicable {
    Humpback humpback;
    long sp;
    ConcurrentHashMap<Integer, TableStats> stats = new ConcurrentHashMap<>();
    long lastSaveTime;
    private long commitedLp;

    public Statistician(Humpback humpback) {
        this.humpback = humpback;
        this.sp = this.humpback.getCheckPoint().getStatisticanLogPointer();
        if (this.sp == 0) {
            this.sp = this.humpback.getGobbler().getStartSp();
        }
        this.commitedLp = this.sp;
        load();
    }

    @Override
    public void insert(InsertEntry entry) throws Exception {
        long pRow = entry.getRowPointer();
        Row row = Row.fromMemoryPointer(pRow, 0);
        int tableId = entry.getTableId();
        if (tableId == Humpback.SYSSTATS_TABLE_ID) {
            return;
        }
        TableStats table = getTableStats(tableId);
        table.inspectInsert(row, entry.sp);
        this.sp = entry.sp;
    }

    @Override
    public void update(UpdateEntry entry) throws Exception {
        long pRow = entry.getRowPointer();
        Row row = Row.fromMemoryPointer(pRow, 0);
        int tableId = entry.getTableId();
        if (tableId == Humpback.SYSSTATS_TABLE_ID) {
            return;
        }
        TableStats table = getTableStats(tableId);
        table.inspectUpdate(row, entry.sp);
        this.sp = entry.sp;
    }

    @Override
    public void put(PutEntry entry) throws Exception {
        long pRow = entry.getRowPointer();
        Row row = Row.fromMemoryPointer(pRow, 0);
        int tableId = entry.getTableId();
        if (tableId == Humpback.SYSSTATS_TABLE_ID) {
            return;
        }
        TableStats table = getTableStats(tableId);
        table.inspectPut(row, entry.sp);
        this.sp = entry.sp;
    }

    @Override
    public void index(IndexEntry entry) throws Exception {
        this.sp = entry.sp;
    }

    @Override
    public void delete(DeleteEntry entry) throws Exception {
        int tableId = entry.getTableId();
        if (tableId == Humpback.SYSSTATS_TABLE_ID) {
            return;
        }
        TableStats table = getTableStats(tableId);
        table.inspectDelete(entry.sp);
        this.sp = entry.sp;
    }
    
    @Override
    public void deleteRow(DeleteRowEntry entry) throws Exception {
        int tableId = entry.getTableId();
        if (tableId == Humpback.SYSSTATS_TABLE_ID) {
            return;
        }
        TableStats table = getTableStats(tableId);
        table.inspectDelete(entry.sp);
        this.sp = entry.sp;
    }

    @Override
    public long getReplicateLogPointer() {
        return this.sp;
    }

    @Override
    public ReplicationHandler getReplayHandler() {
        return this;
    }

    @Override
    public void flush() throws Exception {
        if ((UberTime.getTime() - this.lastSaveTime) >= 30 * 1000) {
            save();
        }
    }

    private TableStats getTableStats(int tableId) {
        TableStats result = this.stats.get(tableId);
        if (result == null) {
            result = new TableStats();
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
        GTable gtable = this.humpback.getTable(Humpback.SYSSTATS_TABLE_ID);
        for (TableStats i:this.stats.values()) {
            if (i.isDirty) {
                SlowRow row = i.save();
                gtable.put(1, row, 0);
            }
        }
        this.lastSaveTime = UberTime.getTime();
        humpback.getCheckPoint().setStatisticanLogPointer(this.sp);
        this.commitedLp = this.sp;
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
            this.sp = Math.max(this.sp, tableStats.sp);
        }
    }

    @Override
    public void putRows(int tableId, List<Long> rows) {
        throw new NotImplementedException();
    }

    @Override
    public void putIndexLines(int tableId, List<Long> indexLines) {
        throw new NotImplementedException();
    }

    @Override
    public void deletes(int tableId, List<Long> deletes) {
        throw new NotImplementedException();
    }
    
    @Override
    public void transactionWindow(TransactionWindowEntry entry) throws Exception {
        this.sp = entry.getSpacePointer();
    }

    @Override
    public long getCommittedLogPointer() {
        return this.commitedLp;
    }
}
