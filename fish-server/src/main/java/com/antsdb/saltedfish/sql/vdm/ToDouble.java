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

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.util.CodingError;

public class ToDouble extends UnaryOperator {

	public ToDouble(Operator upstream) {
		super(upstream);
	}
	
    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long addrVal = this.upstream.eval(ctx, heap, params, pRecord);
        Object val = FishObject.get(heap, addrVal);
        if (val == null) {
        }
        else if (val instanceof Double) {
        }
        else if (val instanceof Float) {
        	val = (double)(Float)val;
        }
        else if (val instanceof String) {
            val = Double.valueOf((String)val);
        }
        else if (val instanceof BigDecimal) {
        	val = ((BigDecimal)val).doubleValue();
        }
        else if (val instanceof Long) {
        	val = ((Long)val).doubleValue();
        }
        else {
            throw new CodingError();
        }
        return FishObject.allocSet(heap, val);
    }

    @Override
    public DataType getReturnType() {
        return DataType.doubleType();
    }
}
