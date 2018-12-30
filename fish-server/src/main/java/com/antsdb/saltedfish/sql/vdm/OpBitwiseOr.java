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

import java.util.Arrays;
import java.util.List;

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Int8;
import com.antsdb.saltedfish.sql.DataType;

/**
 * 
 * @author *-xguo0<@
 */
public class OpBitwiseOr extends BinaryOperator {

    public OpBitwiseOr(Operator left, Operator right) {
        super(left, right);
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long px = this.left.eval(ctx, heap, params, pRecord);
        if (px == 0) {
            return 0;
        }
        long py = this.right.eval(ctx, heap, params, pRecord);
        if (py == 0) {
            return 0;
        }
        long x = AutoCaster.getLong(px);
        long y = AutoCaster.getLong(py);
        long z = x | y;
        return Int8.allocSet(heap, z);
    }

    @Override
    public DataType getReturnType() {
        return DataType.bool();
    }

    @Override
    public List<Operator> getChildren() {
        return Arrays.asList(new Operator[]{left, right});
    }

    @Override
    public String toString() {
        return this.left.toString() + " & " + this.right.toString();
    }
}
