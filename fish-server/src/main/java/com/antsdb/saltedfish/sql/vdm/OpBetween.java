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

import java.util.function.Consumer;

import com.antsdb.saltedfish.cpp.FishBool;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

/**
 * 
 * @author wgu0
 */
public class OpBetween extends Operator {
    Operator expr;
    Operator from;
    Operator to;
    
    public OpBetween(Operator expr, Operator from, Operator to) {
        this.expr = expr;
        this.from = from;
        this.to = to;
    }

    public Operator getLeftOperator() {
        return this.expr;
    }
    
    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pValue = this.expr.eval(ctx, heap, params, pRecord);
        long pFrom = this.from.eval(ctx, heap, params, pRecord);
        long pTo = this.to.eval(ctx, heap, params, pRecord);
        if (AutoCaster.compare(heap, pValue, pFrom) < 0) {
            return FishBool.allocSet(heap, false);
        }
        if (AutoCaster.compare(heap, pValue, pTo) > 0) {
            return FishBool.allocSet(heap, false);
        }
        return FishBool.allocSet(heap, true);
    }

    @Override
    public DataType getReturnType() {
        return DataType.bool();
    }

    @Override
    public void visit(Consumer<Operator> visitor) {
        visitor.accept(this);
        this.expr.visit(visitor);
        this.from.visit(visitor);
        this.to.visit(visitor);
    }

    public Operator getFrom() {
        return this.from;
    }

    public Operator getTo() {
        return this.to;
    }

    @Override
    public String toString() {
        return "BETWEEN " + getFrom().toString() + " AND " + getTo().toString();
    }
}
