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
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.SpaceManager;
import com.antsdb.saltedfish.nosql.Gobbler.DeleteEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.DeleteRowEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.IndexEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.LogEntry;
import com.antsdb.saltedfish.nosql.Gobbler.RowUpdateEntry2;
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
    
    void addRow(int tableId, long pKey, long lpEntry) {
        Map<Long, Long> table = getTable(tableId);
        table.put(pKey, lpEntry);
        this.count++;
    }
    
    void addIndexLine(int tableId, long pKey, long lpEntry) {
        Map<Long, Long> table = getTable(tableId);
        table.put(pKey, lpEntry);
        this.count++;
    }
    
    void addDelete(int tableId, long pKey, long lpEntry) {
        Map<Long, Long> table = getTable(tableId);
        table.put(pKey, lpEntry);
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
        // detect metadata change
        boolean metadataChanged = false;
        for (Map.Entry<Integer, Map<Long, Long>> i:this.tableById.entrySet()) {
            int tableId = i.getKey();
            if (tableId < 0x50) {
                metadataChanged = true;
                break;
            }
        }
        if (metadataChanged) {
            this.updaters.clear();
        }
        
        // update hbase
        int result = 0;
        List<Put> puts = new ArrayList<>();
        List<Delete> deletes = new ArrayList<>();
        SpaceManager sm = this.humpback.getSpaceManager();
        for (Map.Entry<Integer, Map<Long, Long>> i:this.tableById.entrySet()) {
            int tableId = i.getKey();
            HBaseTableUpdater updater = getUpdater(tableId);
            if (updater.isDeleted()) {
                continue;
            }
            puts.clear();
            deletes.clear();
            for (Map.Entry<Long,Long> j:i.getValue().entrySet()) {
                long pKey = j.getKey();
                long lpEntry = j.getValue();
                long pEntry = sm.toMemory(lpEntry);
                LogEntry entry = LogEntry.getEntry(lpEntry, pEntry);
                if (entry instanceof DeleteRowEntry2) {
                    deletes.add(new Delete(Helper.antsKeyToHBase(pKey)));
                }
                else if (entry instanceof RowUpdateEntry2) {
                    RowUpdateEntry2 rowEntry = (RowUpdateEntry2) entry;
                    puts.add(updater.toPut(rowEntry));
                }
                else if (entry instanceof IndexEntry2) {
                    IndexEntry2 indexEntry = (IndexEntry2) entry;
                    puts.add(Helper.toPut(indexEntry));
                }
                else if (entry instanceof DeleteEntry2) {
                    deletes.add(new Delete(Helper.antsKeyToHBase(pKey)));
                }
                else {
                    String msg = String.format("lp=%x type=%s", lpEntry, entry.getClass().getName());
                    throw new IllegalArgumentException(msg);
                }
            }
            if (puts.size() != 0) {
                updater.putRows(puts);
                result += puts.size();
            }
            if (deletes.size() != 0) {
                updater.deletes(deletes);
                result += deletes.size();
            }
        }
        
        // clean up
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

    public void resetUpdaters() {
        this.updaters.clear();
    }
}
