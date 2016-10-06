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

import java.math.BigDecimal;
import java.sql.Timestamp;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.util.CodingError;

/**
 * this is a relaxed version of converting string to timestamp. 
 * 
 * https://dev.mysql.com/doc/refman/5.1/en/datetime.html
 * 
 * @author xguo
 *
 */
public class ToTimestampRelaxed extends UnaryOperator {
    public ToTimestampRelaxed(Operator upstream) {
        super(upstream);
    }

	@Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long addrVal = this.upstream.eval(ctx, heap, params, pRecord);
        return toTimestamp(heap, addrVal);
    }

    @Override
    public DataType getReturnType() {
        return DataType.timestamp();
    }

    @SuppressWarnings("deprecation")
    public static long toTimestamp(Heap heap, long pValue) {
    	int format = Value.getFormat(heap, pValue);
    	if (format == Value.FORMAT_TIMESTAMP) {
    		return pValue;
    	}
        Object val = FishObject.get(heap, pValue);
        if (val instanceof byte[]) {
            val = new String((byte[])val);
        }
        if (val instanceof BigDecimal) {
        	val = ((BigDecimal)val).longValueExact();
        }
        if (val == null) {
        }
        else if (val instanceof Timestamp) {
        }
        else if (val instanceof String) {
        	long pResult = AutoCaster.toTimestamp(heap, pValue);
        	return pResult;
        }
        else if (val instanceof Long) {
        	long n = (Long)val;
        	if (n == 0) {
        		val = new Timestamp(Long.MIN_VALUE);
        	}
        	else {
	        	int sec = (int)(n % 100);
	        	n = n / 100;
	        	int min = (int)(n % 100);
	        	n = n / 100;
	        	int hour = (int)(n % 100);
	        	n = n / 100;
	        	int day = (int)(n % 100);
	        	n = n / 100;
	        	int month = (int)(n % 100);
	        	int year = (int)(n / 100);
	        	val = new Timestamp(year-1900, month-1, day, hour, min, sec, 0);
        	}
        }
        else {
            throw new CodingError();
        }
        return FishObject.allocSet(heap, val);
    }
}
