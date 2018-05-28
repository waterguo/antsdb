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

import com.antsdb.saltedfish.cpp.Float8;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

/**
 * 
 * @author *-xguo0<@
 */
public class FuncAvg extends Function {
    Operator expr;
    int variableId;

    private static class Data {
        double sum;
        long count = 0;
    }
    
    public FuncAvg(int variableId, Operator expr) {
        super();
        this.variableId = variableId;
        this.expr = expr;
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        Data data = (Data)ctx.getVariable(this.variableId);
        if (data == null) {
            data = init(ctx);
        }
        if (Record.isGroupEnd(pRecord)) {
            data.sum = 0;
            this.expr.eval(ctx, heap, params, pRecord);
            double avg = (data.count == 0) ? 0 : data.sum/data.count;
            return Float8.allocSet(heap, avg);
        }
        long addrValue = this.expr.eval(ctx, heap, params, pRecord);
        if (addrValue != 0) {
            // sql standards skips null
            data.sum += AutoCaster.getDouble(addrValue);
            data.count++;
        }
        double avg = (data.count == 0) ? 0 : data.sum/data.count;
        return Float8.allocSet(heap, avg);
    }

    private Data init(VdmContext ctx) {
        Data data = new Data();
        ctx.setVariable(variableId, data);
        return data;
    }

    @Override
    public DataType getReturnType() {
        return DataType.doubleType();
    }

    @Override
    public int getMinParameters() {
        return 1;
    }

}
