/*-------------------------------------------------------------------------------------------------
 _______ __   _ _______ _______ ______  ______
 |_____| | \  |    |    |______ |     \ |_____]
 |     | |  \_|    |    ______| |_____/ |_____]

 Copyright (c) 2016, antsdb.com and/or its affiliates. All rights reserved. *-xguo0<@

 This program is free software: you can redistribute it and/or modify it under the terms of the
 GNU Affero General Public License, version 3, as published by the Free Software Foundation.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/agpl-3.0.txt>
-------------------------------------------------------------------------------------------------*/
package com.antsdb.saltedfish.nosql;

import java.io.File;
import java.io.IOException;

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.cpp.Bytes;

public final class GTable implements AutoCloseable {
    String namespace;
    MemTable memtable;
    int id;
    Humpback humpback;

    public GTable(Humpback owner, String namespace, int id, int fileSize) throws IOException {
    	this.id = id;
    	this.namespace = namespace;
    	this.humpback = owner;
    	this.memtable = new MemTable(owner, new File(owner.data, namespace), id, fileSize);
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
	public HumpbackError insertIndex(long trxid, long pIndexKey, long pRowKey, int timeout) {
		return this.memtable.insertIndex(trxid, pIndexKey, pRowKey, timeout);
	}
    
	public HumpbackError insertIndex_nologging(long trxid, long pIndexKey, long pRowKey, long sp, int timeout) {
		return this.memtable.insertIndex_nologging(trxid, pIndexKey, pRowKey, sp, timeout);
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
    
    public HumpbackError put(long trxid, SlowRow row) {
    	try (BluntHeap heap = new BluntHeap()) {
    		VaporizingRow vrow = row.toVaporisingRow(heap);
    		vrow.setVersion(trxid);
    		return put(vrow);
    	}
    }
    
    public HumpbackError put(VaporizingRow row) {
    	return this.memtable.put(row);
    }
    
	public HumpbackError putNoLogging(long trxid, long pKey, long spRow) {
		return this.memtable.putNoLogging(trxid, pKey, spRow);
	}
	
    public HumpbackError delete(long trxid, byte[] key, int timeout) {
    	try (BluntHeap heap = new BluntHeap()) {
    		long pKey = Bytes.allocSet(heap, key);
    		return delete(trxid, pKey, timeout);
    	}
    }
    
    public HumpbackError delete(long trxid, long pKey, int timeout) {
        return this.memtable.delete(trxid, pKey, timeout);
    }
    
    public HumpbackError deleteNoLogging(long trxid, long pKey, long sprow, int timeout) {
    	return this.memtable.deleteNoLogging(trxid, pKey, sprow, timeout);
    }
    
	public long getIndex(long trxid, long trxts, byte[] indexKey) {
		try (BluntHeap heap = new BluntHeap()) {
			long pKey = Bytes.allocSet(heap, indexKey);
	        long row = this.memtable.getIndex(trxid, trxts, pKey);
	        return row;
		}
	}
	
	public long getIndex(long trxid, long version, long pKey) {
		return this.memtable.getIndex(trxid, version, pKey);
	}
	
    public long get(long trxid, long trxts, byte[] key) {
		try (BluntHeap heap = new BluntHeap()) {
			long pKey = Bytes.allocSet(heap, key);
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
			long pKey = Bytes.allocSet(heap, key);
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
    
    public RowIterator scan(
            long trxid,
            long trxts,
            byte[] from, 
            boolean fromInclusive, 
            byte[] to, 
            boolean toInclusive, 
            boolean isAscending) {
    	try (BluntHeap heap = new BluntHeap()) {
    		long pKeyStart = Bytes.allocSet(heap, from);
    		long pKeyEnd = Bytes.allocSet(heap, to);
            RowIterator result = scan(
                    trxid,
                    trxts,
                    pKeyStart, 
                    fromInclusive, 
                    pKeyEnd, 
                    toInclusive, 
                    isAscending);
            return result;
    	}
    }
    
    public RowIterator scan(
            long trxid,
            long trxts,
            long pFrom, 
            boolean fromInclusive, 
            long pTo, 
            boolean toInclusive, 
            boolean isAscending) {
        RowIterator result = this.memtable.scan(
                trxid,
                trxts,
                pFrom, 
                fromInclusive, 
                pTo, 
                toInclusive, 
                isAscending);
        result = new TombstoneEliminator(result);
        return result;
    }
    
    public RowIterator scan(long trxid, long trxts) {
        RowIterator result = scan(
                trxid,
                trxts,
                0, 
                false, 
                0, 
                false, 
                true);
        return result;
    }

    public void truncate() {
        this.memtable.truncate();
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

	public long getEndRowSpacePointer() {
		return this.memtable.getEndRowSpacePointer();
	}

	@Override
	public String toString() {
		return this.memtable.toString();
	}

	public void carbonizeIfPossible() throws IOException {
		this.memtable.carbonizeIfPossible();
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
		open(false);
	}
	
	public void open(boolean deleteCorruptedFile) throws IOException {
		this.memtable.open(deleteCorruptedFile);
	}
}
