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
import java.util.concurrent.atomic.AtomicLong;

import com.antsdb.saltedfish.cpp.FileBasedHeap;
import com.antsdb.saltedfish.cpp.FishBoundary;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Int8;
import com.antsdb.saltedfish.cpp.OffHeapSkipList;
import com.antsdb.saltedfish.cpp.OffHeapSkipListScanner;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.planner.SortKey;

/**
 * 
 * @author *-xguo0<@
 */
public class IndexedTableScan extends CursorMaker {

    private CursorMaker upstream;
    private Vector key;
    private FuncGenerateKey from;
    private FuncGenerateKey to;
    private KeyMaker keyMaker;
    private int[] keyFields;

    private static class MyCursor extends Cursor {
        Heap heap;
        OffHeapSkipListScanner scanner;
        public boolean isEmpty;

        public MyCursor(CursorMeta meta, Heap heap) {
            super(meta);
            this.heap = heap;
        }

        public void seek(long pKey, long pKeyTo) {
            this.isEmpty = false;
            this.scanner.reset(pKey, pKeyTo, 0);
        }
        
        @Override
        public long next() {
            if (this.isEmpty) {
                return 0;
            }
            long pNext = this.scanner.next();
            if (pNext == 0) return 0;
            long pRecord = Unsafe.getLong(pNext);
            return pRecord;
        }

        @Override
        public void close() {
        }
    }
    
    public IndexedTableScan(CursorMaker upstream, int[] keyFields, Vector v, int makerId) {
        this.upstream = upstream;
        this.keyFields = keyFields;
        this.key = v;
        this.makerId = makerId;
        DataType[] types = new DataType[keyFields.length + 1];
        CursorMeta meta = upstream.getCursorMeta();
        for (int i=0; i<keyFields.length; i++) {
            types[i] = meta.getColumn(keyFields[i]).getType();
        }
        types[types.length-1] = DataType.longtype();
        this.keyMaker = new KeyMaker(types);
        this.from = new FuncGenerateKey(this.keyMaker, key, false);
        this.to = new FuncGenerateKey(this.keyMaker, key, true);
    }
    
    @Override
    public CursorMeta getCursorMeta() {
        return this.upstream.getCursorMeta();
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return false;
    }

    @Override
    public String toString() {
        String list = "";
        for (int i:this.keyFields) {
            list += getCursorMeta().getColumn(i).getName() + ",";
        }
        return "Indexed Table Scan (" + this.upstream.makerId + ")" + "(" + list + ")";
    }

    @Override
    public void explain(int level, List<ExplainRecord> records) {
        ExplainRecord rec = new ExplainRecord(getMakerid(), level, toString(), getScore());
        records.add(rec);
        this.upstream.explain(level, records);
    }

    public void setSeekKey(Vector key) {
        this.key = key;
    }

    @Override
    public void demolish(Cursor c) {
        if (c == null) return;
        MyCursor myc = (MyCursor)c;
        if (myc.heap == null) return;
        myc.heap.close();
        myc.heap = null;
        myc.scanner = null;
    }

    @Override
    public Cursor make(VdmContext ctx, Parameters params, long pMaster, Cursor last) {
        // reuse heap if possible
        MyCursor result = (MyCursor)last;
        Heap heap = result != null ? 
            result.heap :
            new FileBasedHeap(ctx.getHumpback().geTemp(), ctx.getConfig().getMaxHeapSize());
        result = result != null ? result : new MyCursor(getCursorMeta(), heap);
        
        try {
            // get the join key
            long pBoundaryFrom = this.from.eval(ctx, heap, params, pMaster);
            long pBoundaryTo = this.to.eval(ctx, heap, params, pMaster);
            if (pBoundaryFrom == 0) {
                result.isEmpty = true;
            }
            else {
                // buffer the upstream
                if (result.scanner == null) {
                    AtomicLong stats = ctx.getCursorStats(makerId);
                    OffHeapSkipList sl = buffer(ctx, heap, params, pMaster, stats);
                    result.scanner = new OffHeapSkipListScanner(sl);
                }
                
                // find the records
                long pKeyFrom = new FishBoundary(pBoundaryFrom).getKeyAddress();
                long pKeyTo = new FishBoundary(pBoundaryTo).getKeyAddress();
                result.seek(pKeyFrom, pKeyTo);
            }
            heap = null;
            return result;
        }
        finally {
            if ((last == null) && (heap != null)) heap.close();
        }
    }

    private OffHeapSkipList buffer(VdmContext ctx, Heap heap, Parameters params, long pMaster, AtomicLong stats) {
        try (Cursor c = this.upstream.make(ctx, params, pMaster)) {
            long counter = 0;
            long pCounter = Int8.allocSet(heap, 0);
            OffHeapSkipList sl = OffHeapSkipList.alloc(heap);
            long[] values = new long[this.keyMaker.keyFields.size()];
            values[values.length-1] = pCounter;
            for (long pRecord = c.next(); pRecord!=0; pRecord = c.next()){
                pRecord = Record.clone(heap, pRecord);
                for (int j=0; j<this.keyFields.length; j++) {
                    values[j] = Record.getValueAddress(pRecord, this.keyFields[j]);
                }
                Int8.set(heap, pCounter, counter++);
                long pKey = this.keyMaker.make(ctx, heap, values);
                Unsafe.putLong(sl.put(pKey), pRecord);
            }
            stats.set(counter);
            return sl;
        }
    }

    @Override
    public float getScore() {
        return 10;
    }
}
