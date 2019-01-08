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

import com.antsdb.saltedfish.cpp.FishString;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

/**
 * 
 * @author wgu0
 */
public class FuncConcat extends Function {

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pResult = 0;
        for (Operator i:this.parameters) {
            long pValue = i.eval(ctx, heap, params, pRecord);
            if (pValue == 0) {
                return 0;
            }
            pValue = AutoCaster.toString(heap, pValue);
            if (pResult == 0) {
                pResult = pValue;
            }
            else {
                pResult = FishString.concat(heap, pResult, pValue);
            }
        }
        return pResult;
    }

    @Override
    public int getMaxParameters() {
        return Integer.MAX_VALUE;
    }

    @Override
    public DataType getReturnType() {
        return DataType.varchar();
    }

    @Override
    public int getMinParameters() {
        return 1;
    }

}
