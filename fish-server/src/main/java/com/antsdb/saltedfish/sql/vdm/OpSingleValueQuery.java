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

import java.util.function.Consumer;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.OrcaException;

public class OpSingleValueQuery extends Operator {
    CursorMaker select;
    
    public OpSingleValueQuery(CursorMaker maker) {
        super();
        this.select = maker;
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        Cursor cursor = (Cursor)select.run(ctx, params, pRecord);
        try {
            long pRecFromSubQuery = cursor.next();
            if (pRecFromSubQuery == 0) {
                return 0;
            }
            if (cursor.next() != 0) {
                throw new OrcaException("Subquery returns more than 1 row");
            }
            long pValue = Record.get(pRecFromSubQuery, 0);
            long pResult = FishObject.clone(heap, pValue);
            return pResult;
        }
        finally {
            cursor.close();
        }
    }

    @Override
    public DataType getReturnType() {
        return this.select.getCursorMeta().getColumn(0).getType();
    }

    @Override
    public void visit(Consumer<Operator> visitor) {
        visitor.accept(this);
    }

    @Override
    public String toString() {
        return " IN SELECT (...)";
    }
}
