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

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import com.antsdb.saltedfish.cpp.FishBool;
import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.OrcaException;

public class OpLike extends BinaryOperator {

    public OpLike(Operator left, Operator right) {
        super(left, right);
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long addrTextLeft = left.eval(ctx, heap, params, pRecord);
        long addrTextRight = right.eval(ctx, heap, params, pRecord);
        if (addrTextRight == 0) {
            throw new OrcaException();
        }
        if (addrTextLeft == 0) {
            return FishBool.allocSet(heap, false);
        }
        String textRight = (String)FishObject.get(heap, addrTextRight);
        String textLeft = (String)FishObject.get(heap, addrTextLeft);
        Pattern p = compile(textRight);
        boolean result = p.matcher(textLeft).matches();
        return FishBool.allocSet(heap, result);
    }

    @Override
    public DataType getReturnType() {
        return DataType.bool();
    }

    @Override
    public List<Operator> getChildren() {
        return Arrays.asList(new Operator[]{left, right});
    }
    
    public static Pattern compile(String spec) {
        spec = spec.replaceAll("_", ".");
        spec = spec.replaceAll("%", ".*");
        Pattern p = Pattern.compile(spec, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        return p;
    }
}
