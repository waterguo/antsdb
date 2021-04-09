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

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.HumpbackError;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.TrxMan;
import com.antsdb.saltedfish.nosql.VaporizingRow;
import com.antsdb.saltedfish.sql.OrcaException;

/**
 * 
 * @author *-xguo0<@
 */
public class HUpdate extends Statement{
    
    private GTable gtable;
    private CursorMaker maker;
    private List<Integer> columns;
    private List<Operator> values;

    public HUpdate(GTable gtable, CursorMaker maker, List<Integer> columns, List<Operator> values) {
        this.gtable = gtable;
        this.maker = maker;
        this.columns = columns;
        this.values = values;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        Cursor c = (Cursor)maker.run(ctx, params, 0);
        int count = 0;
        try (Heap heap = new BluntHeap()) {
            for (long pRecord = c.next();pRecord != 0;pRecord = c.next()) {
                long pKey = Record.getKey(pRecord);
                Row row = this.gtable.getRow(0, Long.MAX_VALUE, pKey, 0);
                if (row == null) {
                    throw new OrcaException("unable to find row {}", KeyBytes.toString(pKey));
                }
                VaporizingRow vrow = createVRow(ctx, heap, row);
                vrow.setVersion(TrxMan.getNewVersion());
                long error = this.gtable.update(ctx.getHSession(), vrow, row.getVersion(), 0);
                if (!HumpbackError.isSuccess(error)) {
                    throw new OrcaException("unable to update row {} due to {}", KeyBytes.toString(pKey), error); 
                }
                count++;
            }
        }
        return count;
    }

    private VaporizingRow createVRow(VdmContext ctx, Heap heap, Row row) {
        int maxColumnId = row.getMaxColumnId();
        for (int i:this.columns) {
            maxColumnId = Math.max(maxColumnId, i);
        }
        VaporizingRow result = VaporizingRow.from(heap, maxColumnId, row);
        result.setKey(row.getKeyAddress());
        for (int i=0; i<this.columns.size(); i++) {
            int columndId = this.columns.get(i);
            Operator value = this.values.get(i);
            long pValue = value.eval(ctx, heap, null, 0);
            result.setFieldAddress(columndId, pValue);
        }
        return result;
    }

}
