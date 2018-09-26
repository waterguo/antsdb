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
import java.util.concurrent.atomic.AtomicLong;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.sql.DataType;

/**
 * 
 * @author *-xguo0<@
 */
public class HSelect extends View {

    private GTable gtable;
    private int[] mapping;

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
            FieldMeta ii = new FieldMeta(String.valueOf(columns.get(i)), DataType.varchar());
            result.addColumn(ii);
        }
        return result;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        RowIterator i = this.gtable.scan(0, Long.MAX_VALUE, true);
        return new DumbCursor(ctx.getSpaceManager(), meta, i, this.mapping, new AtomicLong());
    }
}
