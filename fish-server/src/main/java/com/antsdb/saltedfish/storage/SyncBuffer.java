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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
final class SyncBuffer {
    static Logger _log = UberUtil.getThisLogger();
    
    Map<Integer, Map<Long, Long>> tableById = new HashMap<>();
    private HBaseStorageService hbase;
    private int capacity;
    private Humpback humpback;
    private int count = 0;
    Map<Integer, HBaseTableUpdater> updaters = new HashMap<>();
    Connection conn;

    static class MyComparator implements Comparator<Long> {
        @Override
        public int compare(Long px, Long py) {
            int result = KeyBytes.compare(px, py);
            return result;
        }
    }
    
    SyncBuffer(Humpback humpback, HBaseStorageService hbase, int capacity) {
        this.hbase = hbase;
        this.capacity = capacity;
        this.humpback = humpback;
    }
    
    void connect() throws IOException {
        if ((this.conn == null) || (this.conn.isClosed())) {
            this.conn = this.hbase.createConnection(); 
            this.updaters.clear();
            _log.info("hbase master {} is connected", conn.getAdmin().getClusterStatus().getMaster());
        }
    }
    
    void addRow(int tableId, long pKey, long pData) {
        Map<Long, Long> table = getTable(tableId);
        table.put(pKey, pData);
        this.count++;
    }
    
    void addIndexLine(int tableId, long pKey, long pIndexLine) {
        Map<Long, Long> table = getTable(tableId);
        table.put(pKey, pIndexLine);
        this.count++;
    }
    
    void addDelete(int tableId, long pKey) {
        Map<Long, Long> table = getTable(tableId);
        table.put(pKey, 0l);
        this.count++;
    }
    
    void clear() {
        this.tableById.clear();
        this.count = 0;
    }
    
    boolean flushIfFull(int tableId) throws IOException {
        if ((this.count >= this.capacity) || detectMetadataChange(tableId)) {
            flush();
            return true;
        }
        else {
            return false;
        }
    }
    
    int flush() throws IOException {
        try {
            return flush0();
        }
        catch (RetriesExhaustedException x) {
            // when this happens, we need to reconnect or hbase client hangs forever
            HBaseUtil.closeQuietly(this.conn);
            this.conn = null;
            throw x;
        }
    }
    
    int flush0() throws IOException {
        int result = 0;
        List<Long> puts = new ArrayList<>();
        List<Long> deletes = new ArrayList<>();
        for (Map.Entry<Integer, Map<Long, Long>> i:this.tableById.entrySet()) {
            int tableId = i.getKey();
            puts.clear();
            deletes.clear();
            for (Map.Entry<Long,Long> j:i.getValue().entrySet()) {
                long pKey = j.getKey();
                long pData = j.getValue();
                if (pData != 0) {
                    puts.add(pData);
                }
                else {
                    deletes.add(pKey);
                }
            }
            if (puts.size() != 0) {
                HBaseTableUpdater updater = getUpdater(tableId);
                SysMetaRow tableInfo = this.humpback.getTableInfo(tableId);
                if (tableInfo.getType() == TableType.DATA) {
                    updater.putRows(puts);
                }
                else {
                    updater.putIndexLines(puts);
                }
                result += puts.size();
            }
            if (deletes.size() != 0) {
                HBaseTableUpdater updater = getUpdater(tableId);
                updater.deletes(deletes);
                result += deletes.size();
            }
        }
        this.tableById.clear();
        this.count = 0;
        return result;
    }
    
    private Map<Long, Long> getTable(int tableId) {
        Map<Long, Long> table = this.tableById.get(tableId);
        if (table == null) {
            table = new TreeMap<>(new MyComparator());
            this.tableById.put(tableId, table);
        }
        return table;
    }
    
    private HBaseTableUpdater getUpdater(int tableId) throws IOException {
        HBaseTableUpdater result = this.updaters.get(tableId);
        if (result == null) {
            result = new HBaseTableUpdater(this.hbase, tableId);
            result.prepare(this.conn);
            this.updaters.put(tableId, result);
        }
        return result;
    }
    
    private boolean detectMetadataChange(int tableId) {
        switch (tableId) {
        case Humpback.SYSMETA_TABLE_ID:
        case Humpback.SYSNS_TABLE_ID:
        case Humpback.SYSCOLUMN_TABLE_ID:
            return true;
        }
        return false;
    }
}
