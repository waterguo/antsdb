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

import java.sql.Timestamp;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Int8;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.vdm.AutoCaster;
import com.antsdb.saltedfish.sql.vdm.Function;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;

/**
 * 
 * @author *-xguo0<@
 */
public class FuncUnixTimestamp extends Function {

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long result = 0;
        if (this.parameters.size() == 0) {
            result = System.currentTimeMillis();
        }
        else {
            long pTimestamp = AutoCaster.toTimestamp(heap, this.parameters.get(0).eval(ctx, heap, params, pRecord));
            Timestamp ts = (Timestamp)FishObject.get(heap, pTimestamp);
            if (ts == null) {
                return 0;
            }
            if (ts.getTime() == Long.MIN_VALUE) {
                return 0;
            }
            result = ts.getTime();
        }
        return Int8.allocSet(heap, result / 1000);
    }

    @Override
    public DataType getReturnType() {
        return DataType.longtype();
    }

    @Override
    public int getMinParameters() {
        return 0;
    }

    @Override
    public int getMaxParameters() {
        return 1;
    }
    
}
