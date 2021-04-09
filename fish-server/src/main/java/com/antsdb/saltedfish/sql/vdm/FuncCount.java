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
import com.antsdb.saltedfish.cpp.Int8;
import com.antsdb.saltedfish.cpp.RecyclableHeap;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.sql.DataType;

public class FuncCount extends AggregationFunction {
    int variableId;

    public FuncCount(int variableId) {
        this.variableId = variableId;
    }

    @Override
    public void feed(VdmContext ctx, RecyclableHeap rheap, Heap theap, Parameters params, long pRecord) {
        long pCounter = ctx.getGroupVariable(this.variableId);
        if (pCounter == 0) {
            rheap.createUnit(8);
            pCounter = rheap.alloc(8);
            ctx.setGroupVariable(this.variableId, pCounter);
        }
        if (pRecord != 0) {
            Operator expr = this.getParameter();
            if (expr != null) {
                long pValue = expr.eval(ctx, theap, params, pRecord);
                // null check
                if (pValue != 0) {
                    Unsafe.getAndAddLong(pCounter, 1);
                }
            }
            else {
                // count(*)
                Unsafe.getAndAddLong(pCounter, 1);
            }
        }
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pCounter = ctx.getGroupVariable(this.variableId);
        long count = pCounter == 0 ? 0 : Unsafe.getLong(pCounter);
        return Int8.allocSet(heap, count);
    }

    @Override
    public DataType getReturnType() {
        return DataType.longtype();
    }

    Operator getParameter() {
        return this.parameters.get(0);
    }

    @Override
    public int getMinParameters() {
        return 1;
    }
}
