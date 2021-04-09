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

import java.io.File;
import java.io.IOException;

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.util.LongLong;

public final class GTable implements AutoCloseable, LogSpan {
    String namespace;
    String name;
    MemTable memtable;
    int id;
    Humpback humpback;
    private TableType type;

    public GTable(Humpback owner, String namespace, String name, int id, int fileSize, TableType type) 
    throws IOException {
        this.id = id;
        this.namespace = namespace;
        this.name = name;
        this.humpback = owner;
        this.memtable = new MemTable(owner, new File(owner.data, namespace), id, fileSize);
        this.type = type;
    }
    
    public void setMutable(boolean isMutable) {
        this.memtable.setMutable(isMutable);
    }
    
    public String getNamespace() {
        return namespace;
    }

    public String getName() {
        return this.name;
    }
    
    public long insert(HumpbackSession hsession, SlowRow row, int timeout) {
        try (BluntHeap heap = new BluntHeap()) {
            return insert(hsession, row.toVaporisingRow(heap), timeout);
        }
    }
    
    public long insert(HumpbackSession hsession, VaporizingRow row, int timeout) {
        hsession.prepareUpdates();
        return this.memtable.insert(hsession, row, timeout);
    }

    /**
     * only used for index
     * 
     * @param trxid
     * @param pKey
     * @return
     */
    public long insertIndex(
            HumpbackSession hsession, 
            long trxid, 
            long pIndexKey, 
            long pRowKey, 
            byte misc, 
            int timeout) {
        hsession.prepareUpdates();
        return this.memtable.insertIndex(hsession, trxid, pIndexKey, pRowKey, misc, timeout);
    }
    
    public long insertIndex(
            HumpbackSession hsession, 
            long trxid, 
            byte[] indexKey, 
            byte[] rowKey, 
            byte misc, 
            int timeout) {
        try (BluntHeap heap = new BluntHeap()){
            long pIndexKey = KeyBytes.allocSet(heap, indexKey).getAddress();
            long pRowKey = KeyBytes.allocSet(heap, rowKey).getAddress();
            return insertIndex(hsession, trxid, pIndexKey, pRowKey, misc, timeout);
        }
    }
    
    public long update(HumpbackSession hsession, long trxid, SlowRow row, int timeout) {
        try (BluntHeap heap = new BluntHeap()) {
            VaporizingRow vrow = row.toVaporisingRow(heap);
            long oldVersion = vrow.getVersion();
            vrow.setVersion(trxid);
            return update(hsession, vrow, oldVersion, timeout);
        }
    }
    
    public long update(HumpbackSession hsession, VaporizingRow row, long oldVersion, int timeout) {
        hsession.prepareUpdates();
        return this.memtable.update(hsession, row, oldVersion, timeout);
    }
    
    public long put(HumpbackSession hsession, long trxid, SlowRow row, int timeout) {
        try (BluntHeap heap = new BluntHeap()) {
            VaporizingRow vrow = row.toVaporisingRow(heap);
            vrow.setVersion(trxid);
            return put(hsession, vrow, timeout);
        }
    }
    
    public long put(HumpbackSession hsession, VaporizingRow row, int timeout) {
        hsession.prepareUpdates();
        return this.memtable.put(hsession, row, timeout);
    }
    
    public long delete(HumpbackSession hsession, long trxid, byte[] key, int timeout) {
        try (BluntHeap heap = new BluntHeap()) {
            long pKey = KeyBytes.allocSet(heap, key).getAddress();
            return delete(hsession, trxid, pKey, timeout);
        }
    }
    
    public long delete(HumpbackSession hsession, long trxid, long pKey, int timeout) {
        hsession.prepareUpdates();
        return this.memtable.delete(hsession, trxid, pKey, timeout);
    }
    
    public long deleteRow(HumpbackSession hsession, long trxid, long pRow, int timeout) {
        hsession.prepareUpdates();
        return this.memtable.deleteRow(hsession, trxid, pRow, timeout);
    }
    
    public long getIndex(long trxid, long trxts, byte[] indexKey) {
        try (BluntHeap heap = new BluntHeap()) {
            long pKey = KeyBytes.allocSet(heap, indexKey).getAddress();
            long row = this.memtable.getIndex(trxid, trxts, pKey);
            return row;
        }
    }
    
    public long getIndex(long trxid, long version, long pKey) {
        return this.memtable.getIndex(trxid, version, pKey);
    }
    
    public long get(long trxid, long trxts, byte[] key) {
        try (BluntHeap heap = new BluntHeap()) {
            long pKey = KeyBytes.allocSet(heap, key).getAddress();
            long row = this.memtable.get(trxid, trxts, pKey, 0);
            return row;
        }
    }
    
    public long get(long trxid, long trxts, long pKey, long options) {
        long row = this.memtable.get(trxid, trxts, pKey, options);
        return row;
    }
    
    public Row getRow(long trxid, long trxts, byte[] key) {
        Row row = null;
        try (BluntHeap heap = new BluntHeap()) {
            long pKey = KeyBytes.allocSet(heap, key).getAddress();
            row = this.memtable.getRow(trxid, trxts, pKey, 0);
            /*
            if ((row == null) && (this.humpback.getHBaseService() != null)) {
                row = this.humpback.getHBaseService().get(this.id, trxid, trxts, pKey);
            }
            */
            return row;
        }
    }
    
    public Row getRow(long trxid, long trxts, long pKey, long options) {
        return this.memtable.getRow(trxid, trxts, pKey, 0);
    }
    
    public RowIterator scan(long trxid, long trxts, byte[] from, byte[] to, long options) {
        try (BluntHeap heap = new BluntHeap()) {
            long pKeyStart = (from != null) ? KeyBytes.allocSet(heap, from).getAddress() : 0;
            long pKeyEnd = (to != null) ? KeyBytes.allocSet(heap, to).getAddress() : 0;
            RowIterator result = scan(trxid, trxts, pKeyStart, pKeyEnd, options);
            return result;
        }
    }
    
    public RowIterator scan(long trxid, long trxts, long pFrom, long pTo, long options) {
        RowIterator result = this.memtable.scan(trxid, trxts, pFrom, pTo, options);
        result = new TombstoneEliminator(result);
        return result;
    }
    
    public RowIterator scan(long trxid, long trxts, boolean asc) {
        long options = asc ? 0 : ScanOptions.descending(0);
        RowIterator result = scan(trxid, trxts, 0, 0, options);
        return result;
    }

    public int getId() {
        return this.id;
    }

    public void testEscape(VaporizingRow row) {
        this.memtable.testEscape(row);
    }

    @Override
    public void close() {
        this.memtable.close();
    }
    
    public void drop() {
        this.memtable.drop();
    }

    boolean validate() {
        return this.memtable.validate();
    }

    @Override
    public String toString() {
        return this.memtable.toString();
    }

    public int carbonfreezeIfPossible(long oldestTrxid) throws IOException {
        return this.memtable.carbonfreezeIfPossible(oldestTrxid);
    }

    public boolean isPureEmpty() {
        return this.memtable.isPureEmpty();
    }

    public long lock(long trxid, long pKey, int timeout) {
        return this.memtable.lock(trxid, pKey, timeout);
    }
    
    /**
     * return number of items in the table including tomb stones 
     * 
     * WARNING: this is not number of rows in the table
     * 
     * @return
     */
    public long size() {
        return this.memtable.size();
    }

    public ConcurrentLinkedList<MemTablet> getTablets() {
        return this.memtable.getTablets();
    }
    
    public MemTable getMemTable() {
        return this.memtable;
    }

    public void open() throws IOException {
        this.memtable.open();
    }
    
    public RowIterator scanDelta(long spStart, long spEnd) {
        return this.memtable.scanDelta(spStart, spEnd);
    }
    
    public TableType getTableType() {
        return this.type;
    }

    @Override
    public LongLong getLogSpan() {
        return this.memtable.getLogSpan();
    }

    public String getLocation(long trxid, long version, byte[] key) {
        try (BluntHeap heap = new BluntHeap()) {
            long pKey = KeyBytes.allocSet(heap, key).getAddress();
            String loc = this.memtable.getLocation(trxid, version, pKey);
            return loc;
        }
    }
}
