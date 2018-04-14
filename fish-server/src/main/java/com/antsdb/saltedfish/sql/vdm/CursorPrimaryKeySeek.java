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

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.SpaceManager;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.planner.SortKey;

/**
 * 
 * @author wgu0
 */
public class CursorPrimaryKeySeek extends CursorMaker {
	TableMeta table;
    CursorMeta meta;
    int[] mapping;
	private CursorMaker select;
	private Vector prefix;

	class MyCursor extends CursorWithHeap {
		Cursor select;
		long[] values;
		long pRow;
		boolean isClosed = false;
		private AtomicLong counter;
		GTable gtable;
		Transaction trx;
		
		public MyCursor(
				SpaceManager memman, 
				GTable gtable,
				Transaction trx, 
				AtomicLong counter) {
			super(CursorPrimaryKeySeek.this);
			this.gtable = gtable;
			this.trx = trx;
			this.counter = counter;
		}

		@Override
		public long next() {
			for (;;) {
				if (isClosed) {
					return 0;
				}
				
				// next scan
				
				nextRow();
				if (this.pRow == 0) {
					continue;
				}
				
				// convert row to record
				
		    	long pRecord = newRecord();
		        Row row = Row.fromMemoryPointer(pRow, 0);
		        Record.setKey(pRecord, row.getKeyAddress());
		        for (int i=0; i<this.meta.getColumnCount(); i++) {
		        	long pValue = row.getFieldAddress(CursorPrimaryKeySeek.this.mapping[i]);
		        	Record.set(pRecord, i, pValue);
		        }
		        this.counter.incrementAndGet();
		        return pRecord;
			}
		}

		private void nextRow() {
			// fetch next value from cursor
			
			long pRec = this.select.next();
			if (pRec == 0) {
				this.pRow = 0;
				close();
				return;
			}
			long pValue = Record.get(pRec, 0);
			
			// calculate key
			
			this.values[this.values.length-1] = pValue;
			KeyMaker keymaker = CursorPrimaryKeySeek.this.table.getKeyMaker();
			long pKey = keymaker.make(getHeap(), values);
			
			// scan !!
			
	        this.pRow = gtable.get(trx.getTrxId(), trx.getTrxTs(), pKey);
		}

		@Override
		public void close() {
			this.isClosed = true;
			super.close();
			this.select.close();
		}
	}
	
    public CursorPrimaryKeySeek(TableMeta table, int makerId) {
    	this.table = table;
        this.meta = CursorMeta.from(table);
        this.mapping = this.meta.getHumpbackMapping();
        setMakerId(makerId);
    }

    @Override
    public CursorMeta getCursorMeta() {
        return this.meta;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        GTable gtable = ctx.getHumpback().getTable(table.getHtableId());
        Transaction trx = ctx.getTransaction();
    	MyCursor c = new MyCursor(
    			ctx.getSpaceManager(), 
    			gtable, 
    			trx, 
    			ctx.getCursorStats(makerId));
    	boolean success = false;
    	try {
        	c.select = this.select.make(ctx, params, pMaster);
        	c.values = new long[this.prefix.getValues().size() + 1];
        	for (int i=0; i<this.prefix.getValues().size(); i++) {
        		c.values[i] = this.prefix.getValues().get(i).eval(ctx, c.getHeap(), params, pMaster);
        	}
    		success = true;
        	return c;
    	}
    	finally {
    		if (!success) {
    			c.close();
    		}
    	}
    }

    @Override
    public String toString() {
        return "Cursor Primary Key Seek (" + this.table.getObjectName() + ")";
    }

    @Override
    public void explain(int level, List<ExplainRecord> records) {
        ExplainRecord rec = new ExplainRecord(getMakerid(), level, toString());
        records.add(rec);
        this.select.explain(level+1, records);
    }

	public void setKey(Vector v, CursorMaker select) {
		this.prefix = v;
		this.select = select;
	}

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return false;
    }

}
