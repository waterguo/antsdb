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
import java.math.RoundingMode;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.util.CodingError;

public class ToBigDecimal extends UnaryOperator {
    int precsion = -1;
    int scale = -1;
    
    public ToBigDecimal(Operator upstream) {
        super(upstream);
    }

    public ToBigDecimal(Operator upstream, int precision, int scale) {
        super(upstream);
        this.precsion = precision;
        this.scale = scale;
    }
    
    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long addrVal = this.upstream.eval(ctx, heap, params, pRecord);
        Object val = FishObject.get(heap, addrVal);
        BigDecimal num = null;
        if (val == null) {
            return 0;
        }
        else if (val instanceof BigDecimal) {
            num = (BigDecimal)val;
        }
        else if (val instanceof String) {
            num = new BigDecimal((String)val);
        }
        else if (val instanceof Integer) {
            num = new BigDecimal((Integer)val);
        }
        else if (val instanceof Long) {
            num = new BigDecimal((Long)val);
        }
        else if (val instanceof Float) {
            num = new BigDecimal((Float)val);
        }
        else if (val instanceof Double) {
            num = new BigDecimal((double)val);
        }
        else {
            throw new CodingError(val.getClass().toString());
        }
        
        // adjust scale if necessary
        
        if (this.scale != -1) {
            num = num.setScale(this.scale, RoundingMode.HALF_UP);
        }
        
        // check precision
        
        if (this.precsion != -1) {
            if (num.precision() > this.precsion) {
                throw new OrcaException("Out of range value: " + num);
            }
        }
        
        return FishObject.allocSet(heap, num);
    }

    @Override
    public DataType getReturnType() {
        return DataType.number();
    }

}
