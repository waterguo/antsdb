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
package com.antsdb.saltedfish.sql.vdm;

import java.util.Collections;
import java.util.List;

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.cpp.FishBoundary;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author wgu0
 */
public class IndexSeek extends CursorMaker implements Seekable {
    CursorMeta meta;
    int[] mapping;
    TableMeta table;
    Operator op;
	IndexMeta index;
	private Vector key;
    
    class MyCursor extends CursorWithHeap {
    	long pRow;

		public MyCursor(long pRow) {
			super(IndexSeek.this.meta);
			this.pRow = pRow;
		}

		@Override
		public long next() {
			if (this.pRow == 0) {
				return 0;
			}
	    	long pRecord = newRecord();
	        Row row = Row.fromMemoryPointer(pRow, 0);
	        Record.setKey(pRecord, row.getKeyAddress());
	        for (int i=0; i<this.meta.getColumnCount(); i++) {
	        	long pValue = row.getFieldAddress(IndexSeek.this.mapping[i]);
	        	Record.set(pRecord, i, pValue);
	        }
	        this.pRow = 0;
	        return pRecord;
		}

		@Override
		public void close() {
			super.close();
		}

    }
    
    public IndexSeek(TableMeta table, IndexMeta index, int makerId) {
    	this.index = index;
        this.table = table;
        this.meta = CursorMeta.from(table);
        this.mapping = this.meta.getHumpbackMapping();
        setMakerId(makerId);
    }

    @Override
    public CursorMeta getCursorMeta() {
        return meta;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        if (this.op == null) {
            return null;
        }
    	try (BluntHeap heap = new BluntHeap()) {
    		
    		// Calculate the key
    		
	        long pBoundary = this.op.eval(ctx, heap, params, pMaster);
	        if (pBoundary == 0) {
	            Cursor c = CursorUtil.toCursor(meta, Collections.emptyList());
	            return c;
	        }
	        
	        // find the key in index
	        
	        Transaction trx = ctx.getTransaction();
	        FishBoundary boundary = new FishBoundary(pBoundary);
	        long pIndexKey = boundary.getKeyAddress();
	        GTable gindex = ctx.getHumpback().getTable(index.getIndexTableId());
	        long pRowKey = gindex.getIndex(trx.getTrxId(), trx.getTrxTs(), pIndexKey);
	        if (pRowKey == 0) {
	            Cursor c = CursorUtil.toCursor(meta, Collections.emptyList());
	            return c;
	        }

	        // find the row 
	        
	        GTable gtable = ctx.getHumpback().getTable(table.getHtableId());
	        long pRow = gtable.get(trx.getTrxId(), trx.getTrxTs(), pRowKey);
	        if (pRow != 0) {
		        ctx.getCursorStats(makerId).incrementAndGet();
	        }
	        return new MyCursor(pRow);
    	}
    }

    @Override
    public String toString() {
        return "Index Seek (" + this.table.getObjectName() + ")";
    }

    @Override
    public void explain(int level, List<ExplainRecord> records) {
        ExplainRecord rec = new ExplainRecord(level, toString());
        rec.setMakerId(makerId);
        records.add(rec);
    }

	@Override
	public void setSeekKey(Vector key) {
		this.key = key;
		this.op = new FuncGenerateKey(this.index.getKeyMaker(), key, false);
	}

	@Override
	public Vector getSeekKey() {
		return this.key;
	}
}
