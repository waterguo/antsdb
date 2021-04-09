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

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.FileBasedHeap;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Int8;
import com.antsdb.saltedfish.cpp.OffHeapSkipList;
import com.antsdb.saltedfish.cpp.OffHeapSkipListScanner;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.planner.SortKey;
import com.antsdb.saltedfish.util.UberUtil;

public class DumbSorter extends CursorMaker {
    final static Logger _log = UberUtil.getThisLogger();
    
    CursorMaker upstream;
    List<Operator> exprs;
    List<Boolean> sortAsc;
    private KeyMaker keyMaker;
    private int posOrderBy;
    private CursorMeta meta = new CursorMeta();

    private static class MyCursor extends Cursor {
        private Heap heap;
        private OffHeapSkipListScanner scanner;

        public MyCursor(Heap heap, OffHeapSkipList sl, CursorMeta meta) {
            super(meta);
            this.heap = heap;
            this.scanner = new OffHeapSkipListScanner(sl);
            this.scanner.reset(0, 0, 0);
        }

        @Override
        public long next() {
            long pValue = this.scanner.next();
            if (pValue == 0) return 0;
            long pRecord = Unsafe.getLong(pValue);
            return pRecord;
        }

        @Override
        public void close() {
            if (this.heap != null) {
                this.heap.close();
                this.heap = null;
            }
        }
    }
    
    public DumbSorter(CursorMaker upstream, Operator expr, boolean asc, int makerId) {
        this(upstream, Collections.singletonList(expr), Collections.singletonList((Boolean)asc), makerId, 0);
    }
    
    public DumbSorter(CursorMaker upstream, List<Operator> exprs, List<Boolean> sortAsc, int makerId, int posOrderBy) {
        super();
        this.upstream = upstream;
        this.exprs = exprs;
        this.sortAsc = sortAsc;
        this.posOrderBy = posOrderBy;
        for (int i=0; i<this.posOrderBy; i++) {
            this.meta.addColumn(upstream.getCursorMeta().getColumn(i));
        }
        setMakerId(makerId);
        // build key maker
        DataType[] types = new DataType[exprs.size() + 1];
        for (int i=0; i<exprs.size(); i++) {
            types[i] = exprs.get(i).getReturnType();
        }
        types[types.length-1] = DataType.integer();
        this.keyMaker = new KeyMaker(types);
        // build negates 
        boolean[] negates = new boolean[types.length];
        for (int i=0; i<sortAsc.size(); i++) {
            negates[i] = !sortAsc.get(i);
        }
        this.keyMaker.setNegate(negates);
    }

    @Override
    public CursorMeta getCursorMeta() {
        return this.meta;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        MyCursor result = null;
        Heap heap = new FileBasedHeap(ctx.getHumpback().geTemp(), ctx.getConfig().getMaxHeapSize());
        try (Cursor cc = this.upstream.make(ctx, params, pMaster)) {
            long counter = 0;
            long pCounter = Int8.allocSet(heap, 0);
            long[] keyValues = new long[this.exprs.size() + 1];
            keyValues[keyValues.length-1] = pCounter;
            OffHeapSkipList sl = OffHeapSkipList.alloc(heap);
            for (long pRecord = cc.next(); pRecord != 0; pRecord = cc.next()) {
                pRecord = Record.clone(heap, pRecord);
                getSortKey(ctx, heap, params, pRecord, keyValues);
                Int8.set(heap, pCounter, counter++);
                long pKey = this.keyMaker.make(heap, keyValues);
                Unsafe.putLong(sl.put(pKey), pRecord);
            }
            result = new MyCursor(heap, sl, getCursorMeta());
            CursorStats stats = ctx.getProfiler(this.makerId);
            if (stats != null) {
                String info = String.format("mem_usage=%d", heap.getCapacity());
                stats.countInput(counter);
                stats.info = info;
            }
            heap = null;
            return result;
        }
        finally {
            if (heap != null) {
                heap.close();
            }
        }
    }

    @Override
    public void explain(int level, List<ExplainRecord> records) {
        super.explain(level, records);
        this.upstream.explain(level+1, records);
    }

    private void getSortKey(VdmContext ctx, Heap heap, Parameters params, long pRecord, long[] values) {
        for (int i=0; i<this.exprs.size(); i++) {
            long pValue = Record.get(pRecord, this.posOrderBy + i);
            values[i] = pValue;
        }
    }
    
    @Override
    public String toString() {
        return getClass().getSimpleName();
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

}
