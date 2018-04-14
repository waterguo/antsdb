/*-------------------------------------------------------------------------------------------------
 _______ __   _ _______ _______ ______  ______
 |_____| | \  |    |    |______ |     \ |_____]
 |     | |  \_|    |    ______| |_____/ |_____]

 Copyright (c) 2016, antsdb.com and/or its affiliates. All rights reserved. *-xguo0<@

 This program is free software: you can redistribute it and/or modify it under the terms of the
 GNU Affero General Public License, version 3, as published by the Free Software Foundation.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/agpl-3.0.txt>
-------------------------------------------------------------------------------------------------*/
package com.antsdb.saltedfish.sql.vdm;

import java.util.List;
import java.util.function.Consumer;

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.util.Pair;

/**
 * 
 * @author *-xguo0<@
 */
public class OpCase extends Operator {

    private List<Pair<Operator, Operator>> whens;
    private Operator alse;
    private Operator value;
    private DataType returnType = DataType.integer();

    public OpCase(Operator value, List<Pair<Operator, Operator>> whens, Operator alse) {
        this.whens = whens;
        this.alse = alse;
        this.value = value;
        for (Pair<Operator,Operator> i:this.whens) {
            if (i.y.getReturnType().getFishType() > this.returnType.getFishType()) {
                this.returnType = i.y.getReturnType();
            }
        }
        if (this.alse != null) {
            if (this.alse.getReturnType().getFishType() > this.returnType.getFishType()) {
                this.returnType = this.alse.getReturnType();
            }
        }
    }
    
    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pResult = eval_(ctx, heap, params, pRecord);
        return pResult;
    }

    private long eval_(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pValue = this.value.eval(ctx, heap, params, pRecord);
        if (pValue != 0) {
            for (Pair<Operator,Operator> i:this.whens) {
                long pWhen = i.x.eval(ctx, heap, params, pRecord);
                if (AutoCaster.equals(heap, pValue, pWhen)) {
                    return i.y.eval(ctx, heap, params, pRecord);
                }
            }
        }
        if (this.alse != null) {
            return this.alse.eval(ctx, heap, params, pRecord);
        }
        else {
            throw new OrcaException("Case not found for CASE statement");
        }
    }

    @Override
    public DataType getReturnType() {
        return this.returnType;
    }

    @Override
    public void visit(Consumer<Operator> visitor) {
        this.value.visit(visitor);
        for (Pair<Operator,Operator> i:this.whens) {
            i.x.visit(visitor);
            i.y.visit(visitor);
        }
        if (alse != null) {
            this.alse.visit(visitor);
        }
    }
}
