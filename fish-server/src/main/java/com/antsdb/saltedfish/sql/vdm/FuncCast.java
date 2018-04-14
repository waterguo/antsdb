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

import java.sql.Date;
import java.sql.Timestamp;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.OrcaException;

public class FuncCast extends UnaryOperator {
    DataType type;

    public FuncCast(DataType type, Operator expr) {
        super(expr);
        this.type = type;
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long addrValue = this.upstream.eval(ctx, heap, params, pRecord);
        Object value = FishObject.get(heap, addrValue);
        if (value == null) {
            return 0;
        }
        else if (type.getJavaType().isInstance(value)) {
            return FishObject.allocSet(heap, value);
        }
        else if (type.getJavaType() == Integer.class) {
            if (value instanceof String) {
                return Integer.valueOf((String) value);
            }
        }
        else if (type.getJavaType() == Long.class) {
            if (value instanceof String) {
                return Long.valueOf((String) value);
            }
        }
        else if (type.getJavaType() == String.class) {
            if (value instanceof byte[]) {
                return FishObject.allocSet(heap, new String((byte[]) value));
            }
            else if (value instanceof Date) {
                if (((Date)value).getTime() == Long.MIN_VALUE) {
                    value = "0000-00-00";
                }
            }
            else if (value instanceof Timestamp) {
                if (((Timestamp)value).getTime() == Long.MIN_VALUE) {
                    value = "0000-00-00 00:00:00";
                }
            }
            return FishObject.allocSet(heap, value.toString());
        }
        else if (type.getJavaType() == Date.class) {
            return AutoCaster.toDate(heap, addrValue);
        }
        else if (type.getJavaType() == Timestamp.class) {
            return AutoCaster.toTimestamp(heap, addrValue);
        }
        throw new OrcaException("unable to cast value " + value + " to " + type.toString());
    }

    @Override
    public DataType getReturnType() {
        return this.type;
    }

}
