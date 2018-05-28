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
    MemTable memtable;
    int id;
    Humpback humpback;
    private TableType type;

    public GTable(Humpback owner, String namespace, int id, int fileSize, TableType type) 
    throws IOException {
        this.id = id;
        this.namespace = namespace;
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

    public HumpbackError insert(SlowRow row, int timeout) {
        try (BluntHeap heap = new BluntHeap()) {
            return insert(row.toVaporisingRow(heap), timeout);
        }
    }
    
    public HumpbackError insert(VaporizingRow row, int timeout) {
        return this.memtable.insert(row, timeout);
    }

    /**
     * only used for index
     * 
     * @param trxid
     * @param pKey
     * @return
     */
    public HumpbackError insertIndex(long trxid, long pIndexKey, long pRowKey, byte misc, int timeout) {
        return this.memtable.insertIndex(trxid, pIndexKey, pRowKey, misc, timeout);
    }
    
    public HumpbackError insertIndex(long trxid, byte[] indexKey, byte[] rowKey, byte misc, int timeout) {
        try (BluntHeap heap = new BluntHeap()){
            long pIndexKey = KeyBytes.allocSet(heap, indexKey).getAddress();
            long pRowKey = KeyBytes.allocSet(heap, rowKey).getAddress();
            return insertIndex(trxid, pIndexKey, pRowKey, misc, timeout);
        }
    }
    
    public HumpbackError insertIndex_nologging(
            long trxid, 
            long pIndexKey, 
            long pRowKey, 
            long sp, 
            byte misc, 
            int timeout) {
        return this.memtable.recoverIndexInsert(trxid, pIndexKey, pRowKey, sp, misc, timeout);
    }
    
    public HumpbackError update(long trxid, SlowRow row, int timeout) {
        try (BluntHeap heap = new BluntHeap()) {
            VaporizingRow vrow = row.toVaporisingRow(heap);
            long oldVersion = vrow.getVersion();
            vrow.setVersion(trxid);
            return update(vrow, oldVersion, timeout);
        }
    }
    
    public HumpbackError update(VaporizingRow row, long oldVersion, int timeout) {
            return this.memtable.update(row, oldVersion, timeout);
    }
    
    public HumpbackError put(long trxid, SlowRow row, int timeout) {
            try (BluntHeap heap = new BluntHeap()) {
                VaporizingRow vrow = row.toVaporisingRow(heap);
                vrow.setVersion(trxid);
                return put(vrow, timeout);
            }
    }
    
    public HumpbackError put(VaporizingRow row, int timeout) {
        return this.memtable.put(row, timeout);
    }
    
    public HumpbackError delete(long trxid, byte[] key, int timeout) {
            try (BluntHeap heap = new BluntHeap()) {
                long pKey = KeyBytes.allocSet(heap, key).getAddress();
                return delete(trxid, pKey, timeout);
            }
    }
    
    public HumpbackError delete(long trxid, long pKey, int timeout) {
        return this.memtable.delete(trxid, pKey, timeout);
    }
    
    public HumpbackError deleteRow(long trxid, long pRow, int timeout) {
        return this.memtable.deleteRow(trxid, pRow, timeout);
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
            long row = this.memtable.get(trxid, trxts, pKey);
            return row;
        }
    }
    
    public long get(long trxid, long trxts, long pKey) {
        long row = this.memtable.get(trxid, trxts, pKey);
        return row;
    }
    
    public Row getRow(long trxid, long trxts, byte[] key) {
        Row row = null;
        try (BluntHeap heap = new BluntHeap()) {
            long pKey = KeyBytes.allocSet(heap, key).getAddress();
            row = this.memtable.getRow(trxid, trxts, pKey);
            /*
            if ((row == null) && (this.humpback.getHBaseService() != null)) {
                row = this.humpback.getHBaseService().get(this.id, trxid, trxts, pKey);
            }
            */
            return row;
        }
    }
    
    public Row getRow(long trxid, long trxts, long pKey) {
        return this.memtable.getRow(trxid, trxts, pKey);
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

    public HumpbackError lock(long trxid, long pKey, int timeout) {
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
