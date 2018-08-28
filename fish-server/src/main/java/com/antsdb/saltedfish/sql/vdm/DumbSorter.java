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
package com.antsdb.saltedfish.sql.vdm;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.sql.planner.SortKey;
import com.antsdb.saltedfish.util.UberUtil;

public class DumbSorter extends CursorMaker {
    CursorMaker upstream;
    List<Operator> exprs;
    List<Boolean> sortAsc;

    class MyComparator implements Comparator<Item> {
        @Override
        public int compare(Item x, Item y) {
            for (int i = 0; i < x.key.length; i++) {
                Object xx = x.key[i];
                Object yy = y.key[i];
                int result = UberUtil.safeCompare(xx, yy);
                if (!DumbSorter.this.sortAsc.get(i)) {
                    result = -result;
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
        long pRecord;
    }
    
    private static class MyCursor extends CursorWithHeap {
        List<Item> items;
        int i = 0;

        public MyCursor(CursorMeta meta) {
            super(meta);
        }

        public Heap getHeap() {
            return super.getHeap();
        }
        
        @Override
        public long next() {
            if (i >= this.items.size()) {
                return 0;
            }
            long pResult = items.get(this.i).pRecord;
            if (pResult != 0) {
                Record.size(pResult);
            }
            this.i++;
            return pResult;
        }

        @Override
        public void close() {
            super.close();
        }
    }
    
    public DumbSorter(CursorMaker upstream, Operator expr, boolean asc, int makerId) {
        this(upstream, Collections.singletonList(expr), Collections.singletonList((Boolean)asc), makerId);
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
        MyCursor result = null;
        boolean success = false;
        try (Cursor cc = this.upstream.make(ctx, params, pMaster)) {
            result = new MyCursor(getCursorMeta());
            Heap heap = result.getHeap();
            List<Item> items = new ArrayList<>();
            for (long pRecord = cc.next(); pRecord != 0; pRecord = cc.next()) {
                if (pRecord != 0) {
                    Record.size(pRecord);
                }
                pRecord = Record.clone(heap, pRecord);
                Item item = new Item();
                item.pRecord = pRecord;
                item.key = getSortKey(ctx, heap, params, pRecord);
                items.add(item);
            }
            counter.addAndGet(items.size());
            Collections.sort(items, new MyComparator());
            result.items = items;
            success = true;
            return result;
        }
        finally {
            if (!success && (result != null)) {
                result.close();
            }
        }
    }

    @Override
    public void explain(int level, List<ExplainRecord> records) {
        ExplainRecord rec = new ExplainRecord(getMakerid(), level, getClass().getSimpleName());
        records.add(rec);
        this.upstream.explain(level+1, records);
    }

    private Object[] getSortKey(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
         Object[] key = new Object[this.exprs.size()];
         int i=0;
         for (Operator expr:this.exprs) {
             long pValue = expr.eval(ctx, heap, params, pRecord);
             Object value = FishObject.get(heap, pValue);
             if ((pValue != 0) && (value == null)) {
                 byte format = Value.getFormat(heap, pValue);
                 if (format == Value.FORMAT_DATE) {
                  // mysql '0000-00-00'
                  value = new Date(1);
                 }
                 else if (format == Value.FORMAT_TIMESTAMP) {
               // mysql '0000-00-00 00:00:00'
              value = new Timestamp(1);
                 }
             }
             key[i] = value;
             i++;
         }
        return key;
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return false;
    }

    public CursorMaker getUpstream() {
        return this.upstream;
    }

}
