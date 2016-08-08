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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.FlexibleHeap;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.util.CursorUtil;
import com.antsdb.saltedfish.util.UberUtil;

public class DumbSorter extends CursorMaker {
    CursorMaker upstream;
    List<Operator> exprs;
    List<Boolean> sortAsc;

    class MyComparator implements Comparator<Item> {
        @Override
        public int compare(Item x, Item y) {
			for (int i=0; i<x.key.length; i++) {
				Object xx = x.key[i];
				Object yy = y.key[i];
                int result = UberUtil.safeCompare(xx, yy);
                if (!DumbSorter.this.sortAsc.get(i)) {
                    result = - result;
                }
                if (result != 0) {
                    return result;
                }
			}
			return 0;
        }

        @Override
        public boolean equals(Object obj) {
            return false;
        }
    }

    private static class Item {
    	Object[] key;
    	Record rec;
    }
    
    public DumbSorter(CursorMaker upstream, List<Operator> exprs, List<Boolean> sortAsc, int makerId) {
        super();
        this.upstream = upstream;
        this.exprs = exprs;
        this.sortAsc = sortAsc;
        setMakerId(makerId);
    }

    @Override
    public CursorMeta getCursorMeta() {
        return this.upstream.getCursorMeta();
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
    	AtomicLong counter = ctx.getCursorStats(makerId);
    	Heap heap = new FlexibleHeap();
    	try (Cursor c = this.upstream.make(ctx, params, pMaster)) {
	        List<Item> items = new ArrayList<>();
	        for (long pRecord = c.next(); pRecord != 0; pRecord = c.next()) {
	        	Item item = new Item();
	        	item.rec = Record.toRecord(pRecord);
	        	heap.reset(0);
	        	item.key = getSortKey(ctx, heap, params, pRecord);
	        	items.add(item);
	        }
	        counter.addAndGet(items.size());
	        Collections.sort(items, new MyComparator());
	        List<Record> rows = new ArrayList<>();
	        for (Item item:items) {
	        	rows.add(item.rec);
	        }
	        Cursor newc = CursorUtil.toCursor(c.getMetadata(), rows);
	        return newc;
    	}
    	finally {
    		heap.free();
    	}
    }

	@Override
    public void explain(int level, List<ExplainRecord> records) {
        ExplainRecord rec = new ExplainRecord(level, getClass().getSimpleName());
        records.add(rec);
        this.upstream.explain(level+1, records);
    }

    private Object[] getSortKey(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
    	Object[] key = new Object[this.exprs.size()];
    	int i=0;
    	for (Operator expr:this.exprs) {
    		long pValue = expr.eval(ctx, heap, params, pRecord);
    		Object value = FishObject.get(heap, pValue);
    		key[i] = value;
    		i++;
    	}
		return key;
	}

}
