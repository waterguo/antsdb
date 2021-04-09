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
import com.antsdb.saltedfish.cpp.RecyclableHeap;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.GeneratorContext;

/**
 * 
 * @author wgu0
 */
public class FuncMin extends AggregationFunction {
    private int varidValue;
    
    public FuncMin(GeneratorContext ctx) {
        this.varidValue = ctx.allocVariable();
    }
    
    @Override
    public void feed(VdmContext ctx, RecyclableHeap rheap, Heap theap, Parameters params, long pRecord) {
        long pCurrentUnit = rheap.getCurrentUnit();
        long pUnit2Free = 0;
        try {
            long pExistValue = ctx.getGroupVariable(this.varidValue);
            long pValue = this.parameters.get(0).eval(ctx, rheap, params, pRecord);
            long pResult = pExistValue;
            if (pValue != 0) {
                int comp = ctx.compare(rheap, pExistValue, pValue);
                if (comp == Integer.MIN_VALUE || comp > 0) {
                    pResult = pValue;
                }
            }
            if (pResult != pExistValue) {
                rheap.markNewUnit(100);
                pResult = FishObject.clone(rheap, pResult);
                ctx.setGroupVariable(this.varidValue, pResult);
                pUnit2Free = pExistValue;
            }
        }
        finally {
            rheap.restoreUnit(pCurrentUnit);
            if (pUnit2Free != 0) rheap.freeUnit(pUnit2Free);
        }
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pResult = ctx.getGroupVariable(this.varidValue);
        return pResult;
    }

    @Override
    public DataType getReturnType() {
        return this.parameters.get(0).getReturnType();
    }

    @Override
    public int getMinParameters() {
        return 1;
    }

}
