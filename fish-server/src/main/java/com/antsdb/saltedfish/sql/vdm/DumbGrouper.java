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

import java.util.List;

import com.antsdb.saltedfish.cpp.FileBasedHeap;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.OffHeapSkipList;
import com.antsdb.saltedfish.cpp.OffHeapSkipListScanner;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.planner.SortKey;

public class DumbGrouper extends CursorMaker {
    static final long GROUP_END = 0;
    CursorMaker upstream;
    List<Operator> exprs;
    private KeyMaker keyMaker;

    private static class MyCursor extends Cursor {
        private Heap heap;
        private OffHeapSkipListScanner scanner;
        private long pNextGroupItem = 1;

        public MyCursor(Heap heap, OffHeapSkipList sl, CursorMeta cursorMeta) {
            super(cursorMeta);
            this.heap = heap;
            this.scanner = new OffHeapSkipListScanner(sl);
            this.scanner.reset(0, 0, 0);
        }

        @Override
        public long next() {
            if (this.pNextGroupItem != 0 && this.pNextGroupItem != 1) {
                long pResult = Unsafe.getLong(this.pNextGroupItem);
                this.pNextGroupItem = Unsafe.getLong(this.pNextGroupItem + 8);
                return pResult;
            }
            if (this.pNextGroupItem == 0) {
                this.pNextGroupItem = 1;
                return GROUP_END;
            }
            long pEntry = this.scanner.next();
            if (pEntry == 0) {
                return 0;
            }
            long pGroupItem = Unsafe.getLong(pEntry);
            long pResult = Unsafe.getLong(pGroupItem);
            this.pNextGroupItem = Unsafe.getLong(pGroupItem + 8);
            return pResult;
        }

        @Override
        public void close() {
            if (this.heap != null) {
                this.heap.close();
                this.heap = null;
            }
        }
    }
    
    public DumbGrouper(CursorMaker upstream, List<Operator> exprs, int makerId) {
        super();
        this.upstream = upstream;
        this.exprs = exprs;
        setMakerId(makerId);
        
        // build key maker
        DataType[] types = new DataType[exprs.size()];
        for (int i=0; i<exprs.size(); i++) {
            types[i] = exprs.get(i).getReturnType();
        }
        this.keyMaker = new KeyMaker(types);
    }

    @Override
    public CursorMeta getCursorMeta() {
        return this.upstream.getCursorMeta();
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        // fetch all rows in memory and group them by group key
        Cursor result = null;
        Heap heap = new FileBasedHeap(ctx.getHumpback().geTemp(), ctx.getConfig().getMaxHeapSize());
        
        try (Cursor cc = this.upstream.make(ctx, params, pMaster)) {
            long counter = 0;
            long[] keyValues = new long[this.exprs.size()];
            OffHeapSkipList sl = OffHeapSkipList.alloc(heap);
            for (long pRecord = cc.next(); pRecord != 0; pRecord = cc.next()) {
                pRecord = Record.clone(heap, pRecord);
                getGroupKey(ctx, heap, params, pRecord, keyValues);
                long pKey = this.keyMaker.make(heap, keyValues);
                long pEntry = sl.put(pKey);
                long pGroupItem = Unsafe.getLong(pEntry);
                if (pGroupItem == 0) {
                    pGroupItem = heap.alloc(16);
                    Unsafe.putLong(pGroupItem, pRecord);
                    Unsafe.putLong(pGroupItem + 8, 0);
                }
                else {
                    long pNextGroupItem = pGroupItem;
                    pGroupItem = heap.alloc(16);
                    Unsafe.putLong(pGroupItem, pRecord);
                    Unsafe.putLong(pGroupItem + 8, pNextGroupItem);
                }
                Unsafe.putLong(pEntry, pGroupItem);
                counter++;
            }
            result = counter != 0 ? new MyCursor(heap, sl, getCursorMeta()) : new EmptyCursor(getCursorMeta());
            ctx.getCursorStats(makerId).addAndGet(counter);
            heap = null;
            return result;
        }
        finally {
            if (heap != null) {
                heap.close();
            }
        }
    }

    void getGroupKey(VdmContext ctx, Heap heap, Parameters params, long pRecord, long[] values) {
        for (int i=0; i<this.exprs.size(); i++) {
            Operator expr = this.exprs.get(i);
            long pValue = expr.eval(ctx, heap, params, pRecord);
            values[i] = pValue;
        }
    }
    
    @Override
    public void explain(int level, List<ExplainRecord> records) {
        super.explain(level, records);
        this.upstream.explain(level+1, records);
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return false;
    }

    public CursorMaker getUpstream() {
        return this.upstream;
    }

    @Override
    public float getScore() {
        return this.upstream.getScore();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();    
    }
}
