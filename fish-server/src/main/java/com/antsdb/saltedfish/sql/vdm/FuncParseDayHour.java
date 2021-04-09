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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Int4;
import com.antsdb.saltedfish.sql.DataType;

/**
 * 
 * @author *-xguo0<@
 */
public class FuncParseDayHour extends Function{
    private static Pattern _ptn = Pattern.compile("\\s*(-?\\d+)\\s+(\\d+)\\s*");
    
    private Operator upstream;

    public FuncParseDayHour(Operator upstream) {
        this.upstream = upstream;
    }

    @Override
    public int getMinParameters() {
        return 1;
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        String s = AutoCaster.getString(heap, this.upstream.eval(ctx, heap, params, pRecord));
        if (s == null) {
            return 0;
        }
        Matcher m = _ptn.matcher(s);
        if (!m.matches()) {
            return 0;
        }
        int day = Integer.parseInt(m.group(1));
        int hour = Integer.parseInt(m.group(2));
        if (day < 0) {
            hour = -hour;
        }
        int result = day * 24 * 60 * 60 + hour * 60 * 60;
        return Int4.allocSet(heap, result);
    }

    @Override
    public DataType getReturnType() {
        return DataType.longtype();
    }
}
