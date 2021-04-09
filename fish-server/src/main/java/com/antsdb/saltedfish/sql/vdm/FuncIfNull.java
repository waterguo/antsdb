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

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

/**
 * 
 * @author wgu0
 */
public class FuncIfNull extends Function {
    public FuncIfNull() {
    }
    
    public FuncIfNull(Operator upstream, Operator replace) {
        addParameter(upstream);
        addParameter(replace);
    }
    
    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pValue = this.parameters.get(0).eval(ctx, heap, params, pRecord);
        if (pValue == 0) {
            long pResult = this.parameters.get(1).eval(ctx, heap, params, pRecord);
            return pResult;
        }
        else {
            return pValue;
        }
    }

    @Override
    public DataType getReturnType() {
        return DataType.getUpCast(this.parameters.get(0).getReturnType(), this.parameters.get(1).getReturnType());
    }

    @Override
    public int getMinParameters() {
        return 2;
    }

}
