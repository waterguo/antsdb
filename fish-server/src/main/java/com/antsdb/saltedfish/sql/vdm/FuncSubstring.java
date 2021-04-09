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

import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.cpp.FishUtf8;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Unicode16;
import com.antsdb.saltedfish.sql.DataType;

/**
 * 
 * @author *-xguo0<@
 */
public class FuncSubstring extends Function {

    @Override
    public int getMinParameters() {
        return 2;
    }

    @Override
    public int getMaxParameters() {
        return 3;
    }

    @Override
    public DataType getReturnType() {
        return DataType.varchar();
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        String input = AutoCaster.getString(heap, this.parameters.get(0).eval(ctx, heap, params, pRecord));
        if (input == null) {
            return 0;
        }
        long pStart = this.parameters.get(1).eval(ctx, heap, params, pRecord);
        if (pStart == 0) {
            return 0;
        }
        int start = AutoCaster.getInt(pStart);
        long pLength = this.parameters.size() > 2 ? this.parameters.get(2).eval(ctx, heap, params, pRecord) : 0;
        int length = (pLength == 0) ? input.length() : AutoCaster.getInt(pLength);
        
        // adjustments 
        if (start == 0) {
            return FishUtf8.allocSet(heap, "");
        }
        start = (start > 0) ? start - 1 : start;
        if (length <= 0) {
            return FishUtf8.allocSet(heap, "");
        }
        int end = (start >= 0) ? start + length : input.length() + start + length;
        
        // done
        String result = StringUtils.substring(input, start, end);
        return Unicode16.allocSet(heap, result);
    }
}
