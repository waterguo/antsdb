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

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;

/**
 * 
 * @author *-xguo0<@
 */
public class FullTextIndexScan extends CursorMaker {

    private TableMeta table;
	private IndexMeta index;
	private CursorMeta meta;
	private int[] mapping;
	private Operator term;
	private KeyMaker keyMaker;
	
	public FullTextIndexScan(TableMeta table, IndexMeta index, int makerId) {
    	this.table = table;
    	this.index = index;
        this.meta = CursorMeta.from(table);
        this.mapping = this.meta.getHumpbackMapping();
        setMakerId(makerId);
        this.keyMaker = KeyMaker.getFullTextIndexKeyMaker();
    }

	@Override
	public CursorMeta getCursorMeta() {
		return this.meta;
	}

	@Override
	public Cursor make(VdmContext ctx, Parameters params, long pMaster) {
		RowIterator it = makeRowIterator(ctx, params, pMaster);
		if (it == null) {
			return new EmptyCursor(getCursorMeta());
		}
        GTable gtable = ctx.getHumpback().getTable(table.getId());
        Transaction trx = ctx.getTransaction();
        IndexCursor cursor = new IndexCursor(
        		ctx.getSpaceManager(), 
        		this.meta, 
        		it, 
        		mapping, 
        		gtable, 
        		trx,
        		ctx.getCursorStats(makerId));
        cursor.setName(this.toString());
        return cursor;
	}

	RowIterator makeRowIterator(VdmContext ctx, Parameters params, long pMaster) {
		if (this.term == null) {
			return null;
		}
		try (BluntHeap heap = new BluntHeap()) {
        	// calculate boundary
        	
        	long pTerm = this.term.eval(ctx, heap, params, pMaster);
        	if (pTerm == 0) {
        		return null;
        	}
        	long pFrom = this.keyMaker.make(heap, pTerm);
        	long pTo = this.keyMaker.makeMax(heap, pTerm);

        	// create index scanner
	        
	        GTable gindex = ctx.getHumpback().getTable(index.getIndexTableId());
	        Transaction trx = ctx.getTransaction();
	        RowIterator it = gindex.scan(
	                trx.getTrxId(), 
	                trx.getTrxTs(),
	                pFrom,
	                true,
	                pTo,
	                true,
	                true);
	        return it;
		}
	}
	
	public void setQueryTerm(Operator term) {
		this.term = term;
	}

}
