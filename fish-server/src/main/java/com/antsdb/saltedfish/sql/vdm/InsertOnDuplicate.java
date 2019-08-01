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

import java.util.List;

import com.antsdb.saltedfish.cpp.FlexibleHeap;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.VaporizingRow;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;

/**
 * 
 * @author *-xguo0<@
 */
public class InsertOnDuplicate extends UpdateBase {
    private InsertSingleRow insert;

    public InsertOnDuplicate(Orca orca, TableMeta table, GTable gtable, List<ColumnMeta> columns, List<Operator> values) {
        super(orca, table, gtable, columns, values);
    }

    public void setInsert(InsertSingleRow insert) {
        this.insert = insert;
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters params) {
        Transaction trx = ctx.getTransaction();
        Object result;
        try (Heap heap = new FlexibleHeap()) {
            heap.reset(0);
            VaporizingRow row = this.insert.genRow(ctx, heap, params, null, 0);
            long pKey = this.indexHandlers.getRowKey(heap, trx, row);
            if (pKey == 0) {
                result = this.insert.run(ctx, params);
            }
            else {
                if (updateSingleRow(ctx, heap, params, pKey)) {
                    result = 1;
                }
                else {
                    throw new IllegalArgumentException();
                }
            }
            return result;
        }
    }
}
