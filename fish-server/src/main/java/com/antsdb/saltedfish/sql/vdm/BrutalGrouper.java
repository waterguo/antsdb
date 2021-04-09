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

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.cpp.FileBasedHeap;
import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.OffHeapSkipList;
import com.antsdb.saltedfish.cpp.OffHeapSkipListScanner;
import com.antsdb.saltedfish.cpp.RecyclableHeap;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.planner.SortKey;

/**
 * a better grouper than DumbGrouper. this one uses memory more wisely. this class is to remember this special year
 *  
 * @author *-xguo0<@
 */
public class BrutalGrouper extends CursorMaker {

    private CursorMaker upstream;
    private List<Operator> keys;
    private KeyMaker keyMaker;
    private List<Operator> exprs;
    private CursorMeta meta;
    private List<AggregationFunction> aggregates;

    private static class SingleRecordCursor extends Cursor {

        private Heap heap;
        private long pRecord;

        public SingleRecordCursor(CursorMeta meta, Heap heap, long pRecord) {
            super(meta);
            this.heap = heap;
            this.pRecord = pRecord;
        }

        @Override
        public long next() {
            if (this.pRecord != 0) {
                long pResult = this.pRecord;
                this.pRecord = 0;
                return pResult;
            }
            else {
                return 0;
            }
        }

        @Override
        public void close() {
            if (this.heap != null) {
                this.heap.close();
                this.heap = null;
                this.pRecord = 0;
            }
        }
        
    }
    
    private class MyCursor extends Cursor {
        private Heap heap;
        private OffHeapSkipListScanner scanner;
        private VdmContext ctx;
        private Parameters params;

        public MyCursor(Heap heap, OffHeapSkipList sl, CursorMeta cursorMeta, VdmContext ctx, Parameters params) {
            super(cursorMeta);
            this.heap = heap;
            this.ctx = ctx;
            this.params = params;
            this.scanner = new OffHeapSkipListScanner(sl);
            this.scanner.reset(0, 0, 0);
        }

        @Override
        public long next() {
            long pEntry = this.scanner.next();
            if (pEntry == 0) {
                return 0;
            }
            else {
                long pGroup = Unsafe.getLong(pEntry);
                if (pGroup == 0) throw new IllegalArgumentException();
                GroupContext gctx = new GroupContext(pGroup);
                ctx.setGroupContext(gctx.getAddress());
                long pRecord = gctx.getRecord();
                long pNewRecord = genRecord(this.ctx, this.heap, this.params, pRecord);
                return pNewRecord;
            }
        }

        @Override
        public void close() {
            if (this.heap != null) {
                this.heap.close();
                this.heap = null;
            }
        }
    }
    
    public BrutalGrouper(CursorMaker upstream, List<Operator> keys, List<AggregationFunction> aggregates, int id) {
        this.upstream = upstream;
        this.keys = keys;
        this.aggregates = aggregates;
        setMakerId(id);
        
        // build key maker
        if (this.keys != null) {
            DataType[] types = new DataType[keys.size()];
            for (int i=0; i<keys.size(); i++) {
                types[i] = keys.get(i).getReturnType();
            }
            this.keyMaker = new KeyMaker(types);
        }
    }
    
    public void setOutput(CursorMeta meta, List<Operator> exprs) {
        this.meta = meta;
        this.exprs = exprs;
    }
    
    @Override
    public CursorMeta getCursorMeta() {
        return this.meta;
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return false;
    }

    @Override
    public float getScore() {
        return this.upstream.getScore() * 1.2f;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        // fetch all rows in memory and group them by group key
        Cursor result = null;
        Heap heap = new FileBasedHeap(ctx.getHumpback().geTemp(), ctx.getConfig().getMaxHeapSize());
        Heap heapTemp = new BluntHeap();
        RecyclableHeap rheap = new RecyclableHeap(heap, true);
        ctx.setGroupHeap(rheap);
        
        try (Cursor cc = this.upstream.make(ctx, params, pMaster)) {
            if (this.keys == null || this.keys.size() == 0) {
                // group all in one
                result = runGroupAll(ctx, params, rheap, heapTemp, cc);
            }
            else {
                // group by key
                result = runGroupByKey(ctx, params, rheap, heapTemp, cc);
            }
            CursorStats stats = ctx.getProfiler(this.makerId);
            if (stats != null) {
                String info = String.format("mem_usage=%d", rheap.getParent().getCapacity());
                info += String.format(" heap_usage=%d", rheap.getUsage());
                info += String.format(" heap_free=%d", rheap.getFree());
                stats.info = info;
            }
            heap = null;
            return result;
        }
        finally {
            if (heap != null) {
                heap.close();
            }
            heapTemp.close();
        }
    }
    
    private Cursor runGroupAll(VdmContext ctx, Parameters params, RecyclableHeap rheap, Heap heapTemp, Cursor cc) {
        long counter = 0;
        int nvariables = ctx.getvariableCount();
        long pLastRecord = 0;
        GroupContext gctx = GroupContext.alloc(rheap, nvariables);
        ctx.setGroupContext(gctx.getAddress());
        for (long pRecord = cc.next(); pRecord != 0; pRecord = cc.next()) {
            heapTemp.reset(0);
            for (AggregationFunction i:this.aggregates) {
                i.feed(ctx, rheap, heapTemp, params, pRecord);
            }
            gctx.count();
            pLastRecord = pRecord;
            counter++;
        }
        long pNewRecord = genRecord(ctx, rheap, params, pLastRecord);
        Cursor result = new SingleRecordCursor(meta, rheap, pNewRecord); 
        ctx.getCursorStats(makerId).addAndGet(counter);
        return result;
    }

    private Cursor runGroupByKey(VdmContext ctx, Parameters params, RecyclableHeap rheap, Heap heapTemp, Cursor cc) {
        long counter = 0;
        long[] keyValues = new long[this.keys.size()];
        OffHeapSkipList sl = OffHeapSkipList.alloc(rheap);
        long pListUnit = rheap.getCurrentUnit();
        int nvariables = ctx.getvariableCount();
        for (long pRecord = cc.next(); pRecord != 0; pRecord = cc.next()) {
            // generate the key
            heapTemp.reset(0);
            getGroupKey(ctx, heapTemp, params, pRecord, keyValues);
            long pKey = this.keyMaker.make(heapTemp, keyValues);
            rheap.restoreUnit(pListUnit);
            long pEntry = sl.put(pKey);
            long pGroup = Unsafe.getLong(pEntry);

            // feed the aggregation functions
            GroupContext gctx;
            if (pGroup != 0) {
                gctx = new GroupContext(pGroup);
            }
            else {
                gctx = GroupContext.alloc(rheap, nvariables);
                Unsafe.putLong(pEntry, gctx.getAddress());
            }
            if (this.aggregates != null) {
                ctx.setGroupContext(gctx.getAddress());
                for (AggregationFunction i:this.aggregates) {
                    i.feed(ctx, rheap, heapTemp, params, pRecord);
                }
            }
            long pExistRecord = gctx.getRecord();
            
            // make a clone of the first upstream record in a group
            if (pExistRecord == 0) {
                pRecord = Record.clone(rheap, pRecord);
                gctx.setRecord(pRecord);
            }
            counter++;
        }
        Cursor result = counter != 0 
                ? new MyCursor(rheap, sl, getCursorMeta(), ctx, params) 
                : new EmptyCursor(getCursorMeta());
        ctx.getCursorStats(makerId).addAndGet(counter);
        return result;
    }

    void getGroupKey(VdmContext ctx, Heap heap, Parameters params, long pRecord, long[] values) {
        for (int i=0; i<this.keys.size(); i++) {
            Operator expr = this.keys.get(i);
            long pValue = expr.eval(ctx, heap, params, pRecord);
            values[i] = pValue;
        }
    }
    
    long genRecord(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pNewRecord = Record.alloc(heap, this.exprs.size());
        Record.setKey(pNewRecord, pRecord != 0 ? Record.getKey(pRecord) : 0);
        for (int i=0; i<this.exprs.size(); i++) {
            Operator it = this.exprs.get(i);
            long pValue = it.eval(ctx, heap, params, pRecord);
            pValue = FishObject.clone(heap, pValue);
            Record.set(pNewRecord, i, pValue);
        };
        return pNewRecord;
    }

    @Override
    public void demolish(Cursor c) {
        super.demolish(c);
    }

    @Override
    public void explain(int level, List<ExplainRecord> records) {
        super.explain(level, records);
        this.upstream.explain(level+1, records);
    }
}
