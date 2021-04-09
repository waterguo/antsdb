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

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;

/**
 * 
 * @author *-xguo0<@
 */
public class NotNullCheck extends UnaryOperator {
    private ColumnMeta column;

    public NotNullCheck(Operator upstream, ColumnMeta column) {
        super(upstream);
        this.column = column;
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pValue = 0;
        if (this.upstream != null) {
            pValue = this.upstream.eval(ctx, heap, params, pRecord);
        }
        if (pValue == 0) {
            throw new OrcaException("column {} is not nullable", column.getColumnName());
        }
        return pValue;
    }

    @Override
    public DataType getReturnType() {
        return this.upstream.getReturnType();
    }
}
