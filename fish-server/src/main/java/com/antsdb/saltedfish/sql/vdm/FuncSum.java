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

import java.math.BigDecimal;
import java.math.BigInteger;

import com.antsdb.saltedfish.cpp.FishNumber;
import com.antsdb.saltedfish.cpp.Float8;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.RecyclableHeap;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.util.CodingError;

public class FuncSum extends AggregationFunction {
    Operator expr;
    DataType returnType;
    int variableId;

    public FuncSum(int variableId, Operator expr) {
        super();
        this.variableId = variableId;
        this.expr = expr;
        DataType upstreamType = this.expr.getReturnType();
        if (upstreamType.getJavaType() == Float.class) {
            this.returnType = DataType.doubleType();
        }
        else if (upstreamType.getJavaType() == Double.class) {
            this.returnType = DataType.doubleType();
        }
        else if (upstreamType.getJavaType() == Integer.class) {
            this.returnType = DataType.number();
        }
        else if (upstreamType.getJavaType() == Long.class) {
            this.returnType = DataType.number();
        }
        else if (upstreamType.getJavaType() == BigInteger.class) {
            this.returnType = DataType.number();
        }
        else if (upstreamType.getJavaType() == BigDecimal.class) {
            this.returnType = DataType.number();
        }
        else if (upstreamType.getJavaType() == String.class) {
            this.returnType = DataType.number();
        }
        else if (upstreamType.getJavaType() == Boolean.class) {
            this.returnType = DataType.number();
        }
        else {
            throw new CodingError(upstreamType.getJavaType().toString());
        }
    }

    @Override
    public void feed(VdmContext ctx, RecyclableHeap rheap, Heap theap, Parameters params, long pRecord) {
        long pExistValue = ctx.getGroupVariable(this.variableId);
        long pValue = this.expr.eval(ctx, theap, params, pRecord);
        if (pValue != 0 || pExistValue != 0) {
            if (pExistValue == 0) {
                rheap.markNewUnit(50);
                pExistValue = rheap.alloc(50);
                ctx.setGroupVariable(this.variableId, pExistValue);
                if (this.returnType.getJavaType() == Double.class) {
                    Float8.set(pExistValue, 0);
                }
                else if (this.returnType.getJavaType() == BigDecimal.class) {
                    FishNumber.set(rheap, pExistValue, BigDecimal.ZERO);
                }
                else {
                    throw new IllegalArgumentException();
                }
            }
            if (this.returnType.getJavaType() == Double.class) {
                double sum = 0;
                if (pExistValue != 0) {
                    sum += AutoCaster.getDouble(pExistValue);
                }
                if (pValue != 0) {
                    sum += AutoCaster.getDouble(pValue);
                }
                Float8.set(pExistValue, sum);
            }
            else if (this.returnType.getJavaType() == BigDecimal.class) {
                BigDecimal sum = BigDecimal.ZERO;
                if (pExistValue != 0) {
                    sum = sum.add(AutoCaster.getDecimal(pExistValue));
                }
                if (pValue != 0) {
                    sum = sum.add(AutoCaster.getDecimal(pValue));
                }
                FishNumber.set(rheap, pExistValue, sum);
            }
        }
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pValue = ctx.getGroupVariable(this.variableId);
        return pValue;
    }

    @Override
    public DataType getReturnType() {
        return DataType.number();
    }

    @Override
    public int getMinParameters() {
        return 1;
    }
}
