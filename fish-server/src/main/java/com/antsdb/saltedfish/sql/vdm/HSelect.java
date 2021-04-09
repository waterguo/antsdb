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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.nosql.ScanOptions;
import com.antsdb.saltedfish.sql.DataType;

/**
 * 
 * @author *-xguo0<@
 */
public class HSelect extends View {

    protected GTable gtable;
    protected int[] mapping;

    public HSelect(GTable gtable, List<Integer> columns) {
        super(createMeta(gtable, columns));
        this.gtable = gtable;
        this.mapping = new int[columns.size()];
        for (int i=0; i<columns.size(); i++) {
            this.mapping[i] = columns.get(i);
        }
    }

    private static CursorMeta createMeta(GTable gtable, List<Integer> columns) {
        CursorMeta result = new CursorMeta();
        for (int i=0; i<columns.size(); i++) {
            int columnId = columns.get(i);
            String name;
            switch (columnId) {
            case -1:
                name = "00";
                break;
            case -2:
                name = "01";
                break;
            default:
                name = String.valueOf(columns.get(i));
                break;
            }
            FieldMeta ii = new FieldMeta(name, DataType.varchar());
            result.addColumn(ii);
        }
        return result;
    }

    protected ExprCursor wrap(Cursor upstream, Parameters params) {
        ExprCursor result = new ExprCursor(meta, upstream, params, new AtomicLong());
        result.exprs = new ArrayList<>();
        for (FieldMeta field:meta.getColumns()) {
            Operator op = new ToString(new FieldValue(field, result.exprs.size()));
            result.exprs.add(op);
        }
        return result;
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        RowIterator i = this.gtable.scan(0, Long.MAX_VALUE, 0, 0, ScanOptions.NO_CACHE);
        Cursor scan = new DumbCursor(meta, i, this.mapping, new AtomicLong());
        ExprCursor result = wrap(scan, params);
        return result;
    }
}
