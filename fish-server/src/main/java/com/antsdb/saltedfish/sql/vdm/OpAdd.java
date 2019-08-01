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

import java.util.Date;

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

public class OpAdd extends BinaryOperator {
    
    public OpAdd(Operator left, Operator right) {
        super(left, right);
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long addrLeftValue = this.left.eval(ctx, heap, params, pRecord);
        long adddrRightValue = this.right.eval(ctx, heap, params, pRecord);
        long addrResult = AutoCaster.plus(heap, addrLeftValue, adddrRightValue);
        return addrResult;
    }

    @Override
    public DataType getReturnType() {
        Class<?> classx = left.getReturnType().getJavaType();
        Class<?> classy = right.getReturnType().getJavaType();
        if (classx == classy) {
            return left.getReturnType();
        } 
        else if (Date.class.isAssignableFrom(classx) && Number.class.isAssignableFrom(classy)) {
            return right.getReturnType();
        }
        else if (Date.class.isAssignableFrom(classy) && Number.class.isAssignableFrom(classx)) {
            return left.getReturnType();
        }
        else {
            return DataType.number();
        }
    }

}
