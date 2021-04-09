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

import com.antsdb.saltedfish.cpp.FishBool;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

public class OpLarger extends BinaryOperator {

    public OpLarger(Operator left, Operator right) {
        super(left, right);
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        int result = ctx.compare(heap, params, pRecord, this.left, this.right);
        if (result == Integer.MIN_VALUE) {
            return 0;
        }
        return FishBool.allocSet(heap, result > 0);
    }

    @Override
    public DataType getReturnType() {
        return DataType.bool();
    }

    @Override
    public String toString() {
        return this.left.toString() + " > " + this.right.toString();
    }

}
