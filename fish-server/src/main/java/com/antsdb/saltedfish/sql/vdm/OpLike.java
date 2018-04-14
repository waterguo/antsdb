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
        String textRight = AutoCaster.getString(heap, addrTextRight);
        String textLeft = AutoCaster.getString(heap, addrTextLeft);
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
        final String escape = "\\\\?*[].()$^{}+|";
        StringBuilder result = new StringBuilder();
        for (int i=0; i<spec.length(); i++) {
            char ch = spec.charAt(i);
            if (ch == '_') {
                result.append('.');
            }
            else if (ch == '%') {
                result.append(".*");
            }
            else {
                if (ch == '\\') {
                    if (++i >= spec.length()) {
                        throw new OrcaException("invalid pattern expression");
                    }
                    ch = spec.charAt(i);
                }
                if (escape.indexOf(ch) >= 0) {
                    result.append('\\');
                    result.append(ch);
                }
                else {
                    result.append(ch);
                }
            }
        }
        Pattern p = Pattern.compile(result.toString(), Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        return p;
    }

    @Override
    public String toString() {
        return this.left.toString() + " LIKE " + this.right.toString();
    }
}
