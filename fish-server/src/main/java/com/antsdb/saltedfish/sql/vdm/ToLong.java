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
import com.antsdb.saltedfish.server.mysql.ErrorMessage;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.util.CodingError;

public class ToLong extends UnaryOperator {

    public ToLong(Operator upstream) {
        super(upstream);
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long addrVal = this.upstream.eval(ctx, heap, params, pRecord);
        final Object val = FishObject.get(heap, addrVal);
        Long result = null;
        if (val == null) {
        }
        else if (val instanceof Long) {
            result = (Long)val;
        }
        else if (val instanceof Integer) {
            result = Long.valueOf((Integer)val);
        }
        else if (val instanceof String) {
            result = ctx.strict(()-> { return Long.parseLong((String)val); });
            if (result == null) result = 0l;
        }
        else if (val instanceof BigInteger) {
            try {
                result = ((BigInteger)val).longValueExact();
            }
            catch (ArithmeticException x) {
                throw new ErrorMessage(22003, "Out of range value " + val.toString());
            }
        }
        else if (val instanceof BigDecimal) {
            try {
                result = ((BigDecimal)val).longValueExact();
            }
            catch (ArithmeticException x) {
                throw new ErrorMessage(22003, "Out of range value " + val.toString());
            }
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
