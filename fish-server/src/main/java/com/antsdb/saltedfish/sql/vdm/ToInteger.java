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

import java.math.BigDecimal;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Int4;
import com.antsdb.saltedfish.cpp.Int8;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.util.CodingError;

public class ToInteger extends UnaryOperator {

    public ToInteger(Operator upstream) {
        super(upstream);
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pValue = this.upstream.eval(ctx, heap, params, pRecord);
        pValue = AutoCaster.toNumber(heap, pValue);
        byte format = Value.getFormat(heap, pValue);
        if (format == Value.FORMAT_INT4) {
        	    return pValue;
        }
        else if (format == Value.FORMAT_INT8) {
        	    long value = Int8.get(heap, pValue);
            	if ((value > Integer.MAX_VALUE) || (value < Integer.MIN_VALUE)) {
            		throw new IllegalArgumentException(String.valueOf(value));
            	}
            	return Int4.allocSet(heap, (int)value);
        }
        Object val = FishObject.get(heap, pValue);
        Object result = null;
        if (val == null) {
        }
        else if (val instanceof Integer) {
            result = val;
        }
        else if (val instanceof Long) {
        	    long n = (Long)val;
            	if ((n > Integer.MAX_VALUE) || (n < Integer.MIN_VALUE)) {
            		throw new IllegalArgumentException(String.valueOf(n));
            	}
            	result = (int)n;
        }
        else if (val instanceof BigDecimal) {
            BigDecimal value = (BigDecimal)val;
            result = value.intValueExact();
        }
        else if (val instanceof Double) {
        	    val = ((Double)val).intValue();
        }
        else {
            throw new CodingError(val.getClass().toGenericString());
        }
        return FishObject.allocSet(heap, result);
    }

    @Override
    public DataType getReturnType() {
        return DataType.integer();
    }

}
