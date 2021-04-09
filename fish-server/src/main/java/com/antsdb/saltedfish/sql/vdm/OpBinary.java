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

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.OrcaException;

public class OpBinary extends UnaryOperator {

    public OpBinary(Operator upstream) {
        super(upstream);
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pValue = this.upstream.eval(ctx, heap, params, pRecord);
        if (pValue == 0) {
            return 0;
        }
        if (Value.getType(heap, pValue) != Value.TYPE_STRING) {
            throw new OrcaException("BINARY opeartor only works on string");
        }
        long pBytes = FishObject.toBytes(heap, pValue);
        return pBytes;
    }

    @Override
    public DataType getReturnType() {
        return DataType.blob();
    }

}
