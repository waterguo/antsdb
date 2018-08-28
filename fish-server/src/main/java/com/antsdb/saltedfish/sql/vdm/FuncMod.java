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

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

/**
 * 
 * @author *-xguo0<@
 */
public class FuncMod extends Function {

    @Override
    public int getMinParameters() {
        return 2;
    }

    @Override
    public int getMaxParameters() {
        return 2;
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long px = this.parameters.get(0).eval(ctx, heap, params, pRecord);
        if (px == 0) {
            return 0;
        }
        long py = this.parameters.get(1).eval(ctx, heap, params, pRecord);
        if (py == 0) {
            return 0;
        }
        long x = AutoCaster.getLong(px);
        long y = AutoCaster.getLong(py);
        if (y == 0) {
            return 0;
        }
        long z = x % y;
        return FishObject.allocSet(heap, z);
    }

    @Override
    public DataType getReturnType() {
        return DataType.longtype();
    }

}
