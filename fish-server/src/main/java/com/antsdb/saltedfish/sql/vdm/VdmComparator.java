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

import java.util.function.IntSupplier;

import com.antsdb.saltedfish.cpp.Bytes;
import com.antsdb.saltedfish.cpp.FishDate;
import com.antsdb.saltedfish.cpp.FishNumber;
import com.antsdb.saltedfish.cpp.FishString;
import com.antsdb.saltedfish.cpp.FishTimestamp;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.cpp.Value;

/**
 * 
 * @author *-xguo0<@
 */
public class VdmComparator {
    private VdmContext ctx;

    public VdmComparator(VdmContext ctx) {
        this.ctx = ctx;
    }

    public Integer comp(Heap heap, Parameters params, long pRecord, Operator x, Operator y) {
        long px = x.eval(ctx, heap, params, pRecord);
        long py = y.eval(ctx, heap, params, pRecord);
        if ((px == 0) || (py == 0)) {
            return null;
        }
        int typex = getType(heap, px, x);
        int typey = getType(heap, py, y);
        if (typex != typey){
            int type = max(typex, typey);
            px = AutoCaster.cast(heap, type, typex, px);
            py = AutoCaster.cast(heap, type, typey, py);
        }
        if ((px == 0) || (py == 0)) {
            return null;
        }
        return comp(heap, px, py, x, y);
    }

    private int getType(Heap heap, long p, Operator op) {
        /* mysql string literal is actually binary, we need to treat them as string in comparison */
        int result;
        if (op instanceof BinaryString) {
            BinaryString bs = (BinaryString)op;
            result = bs.isBinary() ? Value.TYPE_BYTES : Value.TYPE_STRING;
        }
        else {
            result = Value.getType(heap, p);
        }
        return result;
    }

    private int comp(Heap heap, long px, long py, Operator x, Operator y) {
        byte typex = Unsafe.getByte(px);
        byte typey = Unsafe.getByte(py);
        if ((typex == 0) || (typey == 0)) {
            throw new IllegalArgumentException();
        }
        
        byte kind = Value.getType(typex);
        
        // ok go ahead diving into types
        
        switch(kind) {
        case Value.TYPE_NUMBER:
            return FishNumber.compare(px, py);
        case Value.TYPE_STRING:
            return compString(px, py, x, y);
        case Value.TYPE_BYTES:
            return Bytes.compare(px, py);
        case Value.TYPE_DATE:
            return FishDate.compare(px, py);
        case Value.TYPE_TIMESTAMP:
            return FishTimestamp.compare(px, py);
        default:
            throw new IllegalArgumentException();
        }
    }

    /* string comparison in sql is special, we need to ignore trailing spaces */
    private int compString(long px, long py, Operator x, Operator y) {
        IntSupplier scanX = FishString.scan(px);
        IntSupplier scanY = FishString.scan(py);
        for (;;) {
            int chx = scanX.getAsInt();
            int chy = scanY.getAsInt();
            if ((chx == -1) && (chy == -1)) {
                break;
            }
            // skip trailing space
            if (chx == -1) {
                if (chy == ' ') {
                    continue;
                }
            }
            if (chy == -1) {
                if (chx == ' ') {
                    continue;
                }
            }
            int result = chx - chy;
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    private final static int max(int typex, int typey) {
        int type;
        if ((typex == Value.TYPE_NUMBER) || (typey == Value.TYPE_NUMBER)) {
            type = Value.TYPE_NUMBER;
        }
        else if ((typex == Value.TYPE_TIMESTAMP) || (typey == Value.TYPE_TIMESTAMP)) {
            type = Value.TYPE_TIMESTAMP;
        }
        else if ((typex == Value.TYPE_DATE) || (typey == Value.TYPE_DATE)) {
            type = Value.TYPE_DATE;
        }
        else if ((typex == Value.TYPE_BYTES) || (typey == Value.TYPE_BYTES)) {
            type = Value.TYPE_BYTES;
        }
        else {
            throw new IllegalArgumentException();
        }
        return type;
    }
}
