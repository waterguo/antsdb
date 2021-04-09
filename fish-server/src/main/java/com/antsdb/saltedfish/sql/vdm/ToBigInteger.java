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
import java.math.BigInteger;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.util.CodingError;

/**
 * 
 * @author *-xguo0<@
 */
public class ToBigInteger extends UnaryOperator {

    public ToBigInteger(Operator upstream) {
        super(upstream);
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long addrVal = this.upstream.eval(ctx, heap, params, pRecord);
        final Object val = FishObject.get(heap, addrVal);
        BigInteger result = null;
        if (val == null) {
        }
        else if (val instanceof BigInteger) {
            result = (BigInteger)val;
        }
        else if (val instanceof Long) {
            result = BigInteger.valueOf((Long)val);
        }
        else if (val instanceof Integer) {
            result = BigInteger.valueOf((Integer)val);
        }
        else if (val instanceof BigDecimal) {
            result = ((BigDecimal)val).toBigIntegerExact();
        }
        else if (val instanceof String) {
            result = ctx.strict(()-> { return new BigInteger((String)val); });
            if (result == null) result = BigInteger.valueOf(0);
        }
        else {
            throw new CodingError();
        }
        return FishObject.allocSet(heap, result);
    }

    @Override
    public DataType getReturnType() {
        return DataType.integer();
    }
}
