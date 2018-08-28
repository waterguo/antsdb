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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import com.antsdb.saltedfish.cpp.FishBool;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

public class OpInValues extends BinaryOperator {
    List<Operator> values = new ArrayList<Operator>();
    
    public OpInValues(Operator left, List<Operator> values) {
        super(left, null);
        this.values = values;
    }

    @Override
    public long eval(VdmContext ctx, Heap heap,  Parameters params, long pRecord) {
        long addrValueLeft = this.left.eval(ctx, heap, params, pRecord);
        for (Operator i:this.values) {
            long addrValueRight = i.eval(ctx, heap, params, pRecord);
            if (AutoCaster.equals(heap, addrValueLeft, addrValueRight)) {
                return FishBool.allocSet(heap, true);
            }
        }
        return FishBool.allocSet(heap, false);
    }

    @Override
    public DataType getReturnType() {
        return DataType.bool();
    }

    @Override
    public void visit(Consumer<Operator> visitor) {
        visitor.accept(this);
        this.left.visit(visitor);
        for (Operator i:this.values) {
            i.visit(visitor);
        }
    }

    @Override
    public String toString() {
        return "IN (...)";
    }

    public List<Operator> getValues() {
        return this.values;
    }
}
