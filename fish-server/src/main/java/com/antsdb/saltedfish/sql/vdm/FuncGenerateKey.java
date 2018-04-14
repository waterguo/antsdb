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

import java.util.List;
import java.util.function.Consumer;

import com.antsdb.saltedfish.cpp.FishBoundary;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;

/**
 * generates a series of bytes from multiple columns. the bytes can be used in indexes. 
 * 
 * @author xguo
 *
 */
public class FuncGenerateKey extends Operator {
    List<Operator> exprs;
    boolean isInclusive;
    KeyMaker keyMaker;
    boolean max;
    private boolean isNullable;

    /**
     * if the key generator accepts null. when this indicator is false, generator will return NULL if one of the
     * value is NULL
     * @param isKeyNullable
     */
    public FuncGenerateKey(KeyMaker keyMaker, Vector v, boolean max) {
        this.keyMaker = keyMaker;
        this.exprs = v.getValues();
        this.isInclusive = v.isInclusive();
        this.isNullable = v.isNullable();
        this.max = max;
    }
    
    public FuncGenerateKey(
            List<ColumnMeta> columns, 
            boolean isUnique, 
            List<Operator> exprs, 
            boolean inclusive, 
            boolean max) {
        super();
        this.exprs = exprs;
        this.keyMaker = new KeyMaker(columns, isUnique);
        this.isInclusive = inclusive;
        this.max = max;
    }
    
    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pKey;
        if (this.max) {
            pKey = this.keyMaker.makeMax(ctx, heap, this.exprs, params, pRecord, this.isNullable);
        }
        else {
            pKey = this.keyMaker.makeMin(ctx, heap, this.exprs, params, pRecord, this.isNullable);
        }
        if (pKey == 0) {
            return 0;
        }
        long pBoundary = FishBoundary.alloc(heap, this.isInclusive, pKey);
        return pBoundary;
    }

    @Override
    public DataType getReturnType() {
        return DataType.blob();
    }

    @Override
    public void visit(Consumer<Operator> visitor) {
        visitor.accept(this);
    }
}
