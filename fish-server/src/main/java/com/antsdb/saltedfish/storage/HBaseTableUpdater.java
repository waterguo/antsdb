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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import com.antsdb.saltedfish.nosql.Gobbler.RowUpdateEntry2;
import com.antsdb.saltedfish.nosql.IndexLine;
import com.antsdb.saltedfish.nosql.Row;

/**
 * batch updater
 * @author *-xguo0<@
 */
class HBaseTableUpdater {

    private int tableId;
    private HBaseStorageService hbase;
    private Mapping mapping;
    private Table htable;
    private TableName tn;

    HBaseTableUpdater(HBaseStorageService hbase, int tableId) {
        this.tableId = tableId;
        this.hbase = hbase;
    }

    void prepare(Connection conn) throws IOException {
        this.tn = hbase.getTableName(tableId);
        if (this.tn != null) {
            this.htable = conn.getTable(this.tn);
            this.mapping = hbase.getMapping(tableId);
        }
    }

    boolean isDeleted() {
        return this.tn == null;
    }
    
    Put toPut(RowUpdateEntry2 entry) {
        long pRow = entry.getRowPointer();
        Row row = Row.fromMemoryPointer(pRow, 0);
        Put put = Helper.toPut(mapping, row, entry.getSpacePointer());
        return put;
    }
    
    void putRows(List<Put> puts) throws IOException {
        if (!this.hbase.isMutable) {
            throw new OrcaHBaseException("hbase storage is in read-only mode");
        }
        if (puts.size() <= 0) {
            return;
        }
        if (isDeleted()) {
            return;
        }
        htable.put(puts);
    }
    
    public void putIndexLines(List<Long> indexLines) throws IOException {
        if (!this.hbase.isMutable) {
            throw new OrcaHBaseException("hbase storage is in read-only mode");
        }
        if (indexLines.size() <= 0) {
            return;
        }
        if (isDeleted()) {
            return;
        }
        List<Put> puts = new ArrayList<>();
        for (Long pLine:indexLines) {
            IndexLine line = IndexLine.from(pLine);
            Put put = Helper.toPut(line);
            puts.add(put);
        }
        this.htable.put(puts);
    }

    public void deletes(List<Delete> deletes) throws IOException {
        if (!this.hbase.isMutable) {
            throw new OrcaHBaseException("hbase storage is in read-only mode");
        }
        if (deletes.size() <= 0) {
            return;
        }
        if (isDeleted()) {
            return;
        }
        htable.delete(deletes);
    }
    
    @Override
    public String toString() {
        return String.format("%s(%d)", this.tn.toString(), this.tableId);
    }
}
