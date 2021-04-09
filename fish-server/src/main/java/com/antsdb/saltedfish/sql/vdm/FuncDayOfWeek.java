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

import java.sql.Timestamp;

import com.antsdb.saltedfish.cpp.FishTimestamp;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Int4;
import com.antsdb.saltedfish.sql.DataType;

/**
 * 
 * @author *-xguo0<@
 */
public class FuncDayOfWeek extends Function {

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pValue = this.parameters.get(0).eval(ctx, heap, params, pRecord);
        if (pValue == 0) return 0;
        pValue = AutoCaster.toTimestamp(heap, pValue);
        Timestamp ts = FishTimestamp.get(heap, pValue);
        int value = ts.toLocalDateTime().getDayOfWeek().getValue() + 1;
        if (value == 8) value = 1;
        return Int4.allocSet(heap, value);
    }

    @Override
    public int getMinParameters() {
        return 1;
    }

    @Override
    public DataType getReturnType() {
        return DataType.integer();
    }

}
