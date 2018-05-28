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

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.util.CodingError;

public class ToTimestamp extends UnaryOperator {

    public ToTimestamp(Operator upstream) {
        super(upstream);
        this.upstream = upstream;
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long addrVal = this.upstream.eval(ctx, heap, params, pRecord);
        Object val = FishObject.get(heap, addrVal);
        if (val == null) {
        }
        else if (val instanceof Timestamp) {
        }
        else if (val instanceof String) {
            val = Timestamp.valueOf(val.toString());
        }
        else {
            throw new CodingError();
        }
        return FishObject.allocSet(heap, val);
    }

    @Override
    public DataType getReturnType() {
        return DataType.timestamp();
    }

}
