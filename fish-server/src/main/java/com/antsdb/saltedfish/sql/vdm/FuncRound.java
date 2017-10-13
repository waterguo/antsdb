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

import java.math.BigDecimal;
import java.math.RoundingMode;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

/**
 * 
 * @author *-xguo0<@
 */
public class FuncRound extends Function {

    @Override
    public int getMinParameters() {
        return 1;
    }

    @Override
    public int getMaxParameters() {
        return 2;
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pValue = AutoCaster.toNumber(heap, this.parameters.get(0).eval(ctx, heap, params, pRecord));
        if (pValue == 0) {
            return 0;
        }
        int scale = getScale(ctx, heap, params, pRecord);
        BigDecimal value = AutoCaster.getDecimal(pValue);
        BigDecimal result = value.setScale(scale, RoundingMode.HALF_UP);
        return FishObject.allocSet(heap, result);
    }

    @Override
    public DataType getReturnType() {
        return DataType.number();
    }

    public int getScale(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        if (this.parameters.size() < 2) {
            return 0;
        }
        long pValue = this.parameters.get(1).eval(ctx, heap, params, pRecord);
        int place = AutoCaster.getInt(pValue);
        return place;
    }
}
