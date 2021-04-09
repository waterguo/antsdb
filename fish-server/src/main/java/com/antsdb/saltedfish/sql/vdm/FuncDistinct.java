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

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.OffHeapSkipList;
import com.antsdb.saltedfish.cpp.RecyclableHeap;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.sql.DataType;

/**
 * mysql count distinct
 * 
 * @see <a href="https://dev.mysql.com/doc/refman/5.0/en/group-by-functions.html#function_count-distinct">count distinct</a>
 * 
 * @author xguo
 *
 */
public class FuncDistinct extends UnaryOperator {
    private int variableId;
    private KeyMaker keymaker;
    
    public FuncDistinct(Operator expr, int variableId) {
        super(expr);
        this.variableId = variableId;
        this.keymaker = new KeyMaker(new DataType[] {DataType.varchar()});
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        RecyclableHeap rheap = ctx.getGroupHeap();
        long pCurrentUnit = rheap.getCurrentUnit();
        try {
            long pList = ctx.getGroupVariable(this.variableId);
            OffHeapSkipList list;
            if (pList != 0) {
                rheap.restoreUnit(pList);
                list = new OffHeapSkipList(rheap, pList);
            }
            else  {
                rheap.markNewUnit(60);
                list = OffHeapSkipList.alloc(rheap);
                pList = list.getAddress();
                ctx.setGroupVariable(this.variableId, pList);
            }
            long pResult = 0;
            if (pRecord != 0) {
                long pValue = this.upstream.eval(ctx, heap, params, pRecord);
                if (pValue != 0) {
                    long pKey = this.keymaker.make(heap, pValue);
                    long pEntry = list.put(pKey);
                    if (Unsafe.getLong(pEntry) == 0) {
                        pResult = pValue;
                        Unsafe.putLong(pEntry, 1);
                    }
                }
            }
            return pResult;
        }
        finally {
            rheap.restoreUnit(pCurrentUnit);
        }
    }

    @Override
    public DataType getReturnType() {
        return this.upstream.getReturnType();
    }
    
}
