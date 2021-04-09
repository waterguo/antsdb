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
import java.time.format.TextStyle;
import java.util.Locale;

import com.antsdb.saltedfish.cpp.FishTimestamp;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Unicode16;
import com.antsdb.saltedfish.sql.DataType;

/**
 * 
 * @author *-xguo0<@
 */
public class FuncMonthName extends Function {

    @Override
    public int getMinParameters() {
        return 1;
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pValue = AutoCaster.toTimestamp(heap, this.parameters.get(0).eval(ctx, heap, params, pRecord));
        if (pValue == 0) {
            return 0;
        }
        Timestamp ts = FishTimestamp.get(heap, pValue);
        String result = ts.toLocalDateTime().getMonth().getDisplayName(TextStyle.FULL, Locale.getDefault());
        return Unicode16.allocSet(heap, result);
    }

    @Override
    public DataType getReturnType() {
        return DataType.varchar();
    }
}
