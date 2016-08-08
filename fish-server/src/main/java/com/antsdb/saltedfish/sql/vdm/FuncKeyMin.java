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

import com.antsdb.saltedfish.cpp.Bytes;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

public class FuncKeyMin extends BinaryOperator {

    public FuncKeyMin(Operator op1, Operator op2) {
        super(op1, op2);
    }
    
    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long addr1 = this.left.eval(ctx, heap, params, pRecord);
        long addr2 = this.right.eval(ctx, heap, params, pRecord);
        byte[] value1 = Bytes.get(heap, addr1);
        byte[] value2 = Bytes.get(heap, addr2);
        byte[] result = KeyUtil.min(value1, value2);
        return (result == value1) ? addr1 : addr2; 
    }

    @Override
    public DataType getReturnType() {
        return DataType.blob();
    }

}
