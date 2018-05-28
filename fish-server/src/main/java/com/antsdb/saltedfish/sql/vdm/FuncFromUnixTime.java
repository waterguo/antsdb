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

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Unicode16;
import com.antsdb.saltedfish.sql.DataType;

/**
 * 
 * @see https://mariadb.com/kb/en/mariadb/from_unixtime/
 * @author *-xguo0<@
 */
public class FuncFromUnixTime extends Function {
    static String _default = "%Y-%m-%d %H:%i:%S";
    
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
        long ts = AutoCaster.getLong(pValue) * 1000;
        String format = getFormat(ctx, heap, params, pRecord);
        String result = FuncDateFormat.format(format, new Timestamp(ts));
        return Unicode16.allocSet(heap, result);
    }

    private String getFormat(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        if (this.parameters.size() < 2) {
            return _default;
        }
        String format = AutoCaster.getString(heap, this.parameters.get(1).eval(ctx, heap, params, pRecord));
        return (format == null) ? _default : format;
    }

    @Override
    public DataType getReturnType() {
        return DataType.varchar();
    }

}
