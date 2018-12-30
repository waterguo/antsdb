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

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

/**
 * 
 * @author *-xguo0<@
 */
public class FuncReplace extends Function {
    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        String x = AutoCaster.getString(heap, this.parameters.get(0).eval(ctx, heap, params, pRecord));
        String y = AutoCaster.getString(heap, this.parameters.get(1).eval(ctx, heap, params, pRecord));
        String z = AutoCaster.getString(heap, this.parameters.get(2).eval(ctx, heap, params, pRecord));
        if ((x == null) || (y == null) || (z == null)) {
            return 0;
        }
        String result = StringUtils.replace(x, y, z);
        return FishObject.allocSet(heap, result);
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
    public int getMinParameters() {
        return 3;
    }
}
