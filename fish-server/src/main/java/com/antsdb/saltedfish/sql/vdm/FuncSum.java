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

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.util.CodingError;

public class FuncSum extends Function {
    Operator expr;
    DataType returnType;
    int variableId;

    private static class Data {
            Object sum;
    }
    
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
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        Data data = (Data)ctx.getVariable(this.variableId);
        if (data == null) {
            data = init(ctx);
        }
        if (Record.isGroupEnd(pRecord)) {
            if (data.sum instanceof Float) {
                data.sum = Float.valueOf(0);
            }
            else if (data.sum instanceof Double) {
                data.sum = Double.valueOf(0);
            }
            else if (data.sum instanceof BigDecimal) {
                data.sum = BigDecimal.ZERO;
            }
            else {
                throw new CodingError();
            }
            this.expr.eval(ctx, heap, params, pRecord);
            return FishObject.allocSet(heap, data.sum);
        }
        long addrValue = this.expr.eval(ctx, heap, params, pRecord);
        addrValue = AutoCaster.toNumber(heap, addrValue);
        Object value = FishObject.get(heap, addrValue);
        if (addrValue == 0) {
            // do nothing
        }
        else if (data.sum instanceof Float) {
            if (value instanceof Float) {
                data.sum = ((Float)data.sum) + ((Float)value);
            }
            else {
                throw new CodingError();
            }
        }
        else if (data.sum instanceof Double) {
            if (value instanceof Float) {
                data.sum = ((Double)data.sum) + ((Float)value);
            }
            else if (value instanceof Double) {
                data.sum = ((Double)data.sum) + ((Double)value);
            }
            else {
                throw new CodingError();
            }
        }
        else if (data.sum instanceof BigDecimal) {
            if (value instanceof Integer) {
                    data.sum = ((BigDecimal)data.sum).add(new BigDecimal((Integer)value));
            }
            else if (value instanceof Long) {
                    data.sum = ((BigDecimal)data.sum).add(new BigDecimal((Long)value));
            }
            else if (value instanceof BigInteger) {
                    data.sum = ((BigDecimal)data.sum).add(new BigDecimal((BigInteger)value));
            }
            else if (value instanceof BigDecimal) {
                    data.sum = ((BigDecimal)data.sum).add((BigDecimal)value);
            }
            else if (value instanceof String) {
                    data.sum = ((BigDecimal)data.sum).add(new BigDecimal((String)value));
            }
            else {
                throw new CodingError();
            }
        }
        else {
            throw new CodingError();
        }
        return FishObject.allocSet(heap, data.sum);
    }

    private Data init(VdmContext ctx) {
        Data data = new Data();
        ctx.setVariable(variableId, data);
        if (this.returnType.getJavaType() == Double.class) {
            data.sum = Double.valueOf(0);
        }
        else if (this.returnType.getJavaType() == Float.class) {
            data.sum = Float.valueOf(0);
        }
        else if (this.returnType.getJavaType() == BigDecimal.class) {
            data.sum = BigDecimal.ZERO;
        }
        else {
            throw new CodingError();
        }
        return data;
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
