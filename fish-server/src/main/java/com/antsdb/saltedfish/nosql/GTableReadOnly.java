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

/**
 * 
 * @author wgu0
 */
public class GTableReadOnly implements AutoCloseable {
    String namespace;
    MemTableReadOnly memtable;
    int id;
    HumpbackReadOnly humpback;

    public GTableReadOnly(HumpbackReadOnly owner, String namespace, int id) 
	throws IOException {
    	this.id = id;
    	this.namespace = namespace;
    	this.humpback = owner;
    	this.memtable = new MemTableReadOnly(owner.getSpaceManager(), new File(owner.data, namespace), id);
    }
    
	public void open() throws IOException {
		this.memtable.open(false);
	}
	
    public String getNamespace() {
        return namespace;
    }

	@Override
	public void close() {
		this.memtable.close();
	}

	public MemTableReadOnly getMemTable() {
		return this.memtable;
	}

    public int getId() {
        return this.id;
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
}
