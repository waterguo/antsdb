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

import java.util.concurrent.atomic.AtomicLong;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.nosql.SpaceManager;

/**
 * 
 * @author wgu0
 */
class IndexCursor extends CursorWithHeap {
	GTable gtable;
    RowIterator iter;
    int[] mapping;
	private SpaceManager memman;
	private boolean isClosed = false;
	Transaction trx;
	private AtomicLong counter;

	public IndexCursor(
			SpaceManager memman, 
			CursorMeta meta, 
			RowIterator iter, 
			int[] mapping, 
			GTable gtable, 
			Transaction trx, 
			AtomicLong counter) {
		super(meta);
		this.gtable = gtable;
		this.trx = trx;
		this.iter = iter;
		this.mapping = mapping;
		this.trx = trx;
		this.memman = memman;
		this.counter = counter;
	}
	
    @Override
    public long next() {
    	if (isClosed) {
    		return 0;
    	}
    	for (;;) {
        	boolean hasNext = iter.next();
        	if (!hasNext) {
        		return 0;
        	}
        	long pRecord = newRecord();
            long pRowKey = iter.getRowKeyPointer();
            if (pRowKey == 0) {
                return 0;
            }
            long pRow = gtable.get(trx.getTrxId(), trx.getTrxTs(), pRowKey);
            if (pRow == 0) {
            	// at rare occasions, row cannot be found. we just ignore it. it could happen for example full text 
            	// index. indexing rows are not deleted completely due to analyzer change
            	continue;
            }
            Row row = Row.fromSpacePointer(this.memman, pRow, 0);
            Record.setKey(pRecord, row.getKeyAddress());
            for (int i=0; i<this.meta.getColumnCount(); i++) {
            	long pValue = row.getFieldAddress(this.mapping[i]);
            	Record.set(pRecord, i, pValue);
            }
            this.counter.incrementAndGet();
            return pRecord;
    	}
    }

    @Override
    public void close() {
    	super.close();
    	this.isClosed  = true;
    }
}

