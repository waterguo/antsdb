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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.planner.SortKey;

/**
 * 
 * @author *-xguo0<@
 */
public class FullTextIndexMergeScan extends CursorMaker {

    private TableMeta table;
	private IndexMeta index;
	private CursorMeta meta;
	private Operator term;
	private int[] mapping;
	
	class MyRowIterator implements RowIterator {

		private List<RowIterator> scans;
		private boolean eof = false;
		private RowIterator lastFetched;
		private long pRow;
		
		MyRowIterator(List<RowIterator> scans) {
			this.scans = scans;
		}
		
		@Override
		public boolean next() {
			if (this.eof) {
				return false;
			}
			
			// 1 step forward
			
			if (lastFetched != null) {
				lastFetched.next();
			}
			else {
				for (RowIterator i:this.scans) {
					i.next();
				}
			}
			
			// find the smallest key in ascending order. or the greatest in descending order 
			
			long rowid = 0;
			for (RowIterator i:this.scans) {
				if (i.eof()) {
					continue;
				}
				long rowidI = i.getIndexSuffix();
				if (rowid == 0) {
					this.pRow = i.getRowPointer();
					this.lastFetched = i;
					rowid = rowidI;
					continue;
				}
				int cmp = Long.compareUnsigned(rowidI, rowid);
				if (cmp < 0) {
					this.pRow = i.getRowPointer();
					this.lastFetched = i;
					rowid = rowidI;
				}
				else if (cmp == 0) {
					// same key. one step forward
					i.next();
				}
			}
			
			// eof detection
			
			if (rowid == 0) {
				this.eof = true;
				this.pRow = 0;
				this.lastFetched = null;
				return false;
			}
			
			// valid value found
			
			return true;
		}

		@Override
		public long getRowKeyPointer() {
			return this.lastFetched.getRowKeyPointer();
		}

		@Override
		public long getIndexSuffix() {
			return this.lastFetched.getIndexSuffix();
		}

		@Override
		public boolean eof() {
			return this.eof;
		}

		@Override
		public long getRowPointer() {
			return this.pRow;
		}

		@Override
		public long getKeyPointer() {
			return this.lastFetched.getKeyPointer();
		}

		@Override
		public long getRowScanned() {
			return this.getRowScanned();
		}

		@Override
		public void rewind() {
			for (RowIterator i:this.scans) {
				i.rewind();
			}
			this.eof = false;
			this.lastFetched = null;
			this.pRow = 0;
		}

		@Override
		public void close() {
			for (RowIterator i:this.scans) {
				i.close();
			}
			this.eof = true;
		}

		@Override
		public byte getMisc() {
			return this.lastFetched.getMisc();
		}
		
	}
	
	public FullTextIndexMergeScan(TableMeta table, IndexMeta index, int makerId) {
    	this.table = table;
    	this.index = index;
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
		if (this.term == null) {
			return new EmptyCursor(getCursorMeta());
		}
		
		// break the queries into terms
		
		Set<String> terms = new HashSet<>(); 
		try (BluntHeap heap = new BluntHeap()) {
			long pValue = this.term.eval(ctx, heap, params, pMaster);
			String query = AutoCaster.getString(heap, pValue);
			LuceneUtil.tokenize(query, (String type, String term) -> {
			    char lead = term.charAt(0);
			    if (lead == '+') {
			        term = term.substring(1);
			    }
				terms.add(term.toLowerCase());
			});
		}
		
		// build the cursor
		
		List<RowIterator> scans = new ArrayList<>();
		for (String term:terms) {
			FullTextIndexScan c = new FullTextIndexScan(table, index, makerId);
			c.setQueryTerm(new StringLiteral(term));
			scans.add(c.makeRowIterator(ctx, params, pMaster));
		}
		MyRowIterator it = new MyRowIterator(scans);
        GTable gtable = ctx.getHumpback().getTable(table.getHtableId());
		Cursor cursor = new IndexCursor(
				ctx.getSpaceManager(), 
				meta, 
				it, 
				mapping, 
				gtable, 
				ctx.getTransaction(), 
				ctx.getCursorStats(makerId));
		return cursor;
	}

	public void setQueryTerm(Operator term) {
		this.term = term;
	}

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return false;
    }

}
