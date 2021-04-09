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

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Int8;
import com.antsdb.saltedfish.cpp.OffHeapSkipList;
import com.antsdb.saltedfish.cpp.OffHeapSkipListScanner;
import com.antsdb.saltedfish.cpp.RecyclableHeap;
import com.antsdb.saltedfish.cpp.Unicode16;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.sql.DataType;

/**
 * 
 * @author wgu0
 */
public class FuncGroupConcat extends AggregationFunction {
    int variableId;
    Boolean asc;
    boolean distinct;
    private Operator separator;
    private KeyMaker keymaker;

    public FuncGroupConcat(int variableId, boolean distinct, Boolean asc) {
        this.variableId = variableId;
        this.distinct = distinct;
        this.asc = asc;
        if (distinct) {
            this.keymaker = new KeyMaker(new DataType[] {DataType.varchar()});
            if (asc != null && !asc) {
                this.keymaker.setNegate(new boolean[] {true});
            }
        }
        else {
            if (asc == null) {
                this.keymaker = new KeyMaker(new DataType[] {DataType.longtype(), DataType.varchar()});
            }
            else {
                this.keymaker = new KeyMaker(new DataType[] {DataType.varchar(), DataType.longtype()});
                if (!asc) {
                    this.keymaker.setNegate(new boolean[] {true, false});
                }
            }
        }
    }

    @Override
    public void feed(VdmContext ctx, RecyclableHeap rheap, Heap theap, Parameters params, long pRecord) {
        long pCurrentUnit = rheap.getCurrentUnit();
        try {
            long pCounter = ctx.getGroupVariable(this.variableId);
            OffHeapSkipList list;
            if (pCounter != 0) {
                rheap.restoreUnit(pCounter);
                list = new OffHeapSkipList(rheap, pCounter + Int8.getSize());
            }
            else  {
                rheap.markNewUnit(60);
                pCounter = Int8.allocSet(rheap, 0);
                list = OffHeapSkipList.alloc(rheap);
                ctx.setGroupVariable(this.variableId, pCounter);
            }
            if (pRecord != 0) {
                long pValue = getValue(ctx, rheap, params, pRecord);
                Int8.set(rheap, pCounter, Int8.get(rheap, pCounter)+1);
                if (pValue != 0) {
                    long pKey = getKey(ctx, rheap, pValue); 
                    long pEntry = list.put(pKey);
                    Unsafe.putLong(pEntry, pValue);
                }
            }
        }
        finally {
            rheap.restoreUnit(pCurrentUnit);
        }
    }

    private long getKey(VdmContext ctx, RecyclableHeap heap, long pValue) {
        long pCounter = ctx.getGroupVariable(this.variableId);
        if (this.distinct) {
            return this.keymaker.make(heap, pValue);
        }
        else {
            if (this.asc == null) {
                return this.keymaker.make(heap, pCounter, pValue);
            }
            else {
                return this.keymaker.make(heap, pValue, pCounter);
            }
        }
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pList = ctx.getGroupVariable(this.variableId);
        if (pList == 0) return 0;
        long pSeparator = getSeparator(ctx, heap, params, pRecord);
        OffHeapSkipList list = new OffHeapSkipList(heap, pList + Int8.getSize());
        OffHeapSkipListScanner scanner = new OffHeapSkipListScanner(list);
        scanner.reset(0, 0, 0);
        long pResult = 0;
        for (long pEntry = scanner.next(); pEntry != 0; pEntry = scanner.next()) {
            long pValue = Unsafe.getLong(pEntry);
            if (pResult != 0) {
                pResult = concat(heap, pResult, pSeparator);
            }
            pResult = concat(heap, pResult, pValue);
        }
        return pResult;
    }

    private long getSeparator(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pSeparator;
        if (this.separator == null) {
            pSeparator = Unicode16.allocSet(heap, ",");
        }
        else {
            pSeparator = this.separator.eval(ctx, heap, params, pRecord);
        }
        return pSeparator;
    }
    
    private long concat(Heap heap, long px, long py) {
        long pResult = 0;
        px = AutoCaster.toUnicode(heap, px);
        py = AutoCaster.toUnicode(heap, py);
        if (px != 0) {
            if (py != 0) {
                pResult = Unicode16.concat(heap, Value.FORMAT_UNICODE16, px, Value.FORMAT_UNICODE16, py);
            }
            else {
                pResult = px;
            }
        }
        else {
            if (py != 0) {
                pResult = py;
            }
        }
        return pResult;
    }
    
    private long getValue(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pResult = 0;
        for (Operator i:this.parameters) {
            long pValue = i.eval(ctx, heap, params, pRecord);
            if (pValue == 0) {
                return 0;
            }
            pResult = concat(heap, pResult, pValue);
        }
        if (this.parameters.size() == 1) {
            // prevents drifting memory
            pResult = FishObject.clone(heap, pResult);
        }
        return pResult;
    }

    @Override
    public DataType getReturnType() {
        return DataType.varchar();
    }

    @Override
    public int getMinParameters() {
        return 1;
    }

    @Override
    public int getMaxParameters() {
        return Integer.MAX_VALUE;
    }

    public void setSeparator(Operator separator) {
        this.separator = separator;
    }
}
