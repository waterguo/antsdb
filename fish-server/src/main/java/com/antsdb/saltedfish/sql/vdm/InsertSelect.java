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

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.FlexibleHeap;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.sql.LockLevel;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class InsertSelect extends InsertSingleRow {
    static Logger _log = UberUtil.getThisLogger();

    private CursorMaker maker;

    public InsertSelect(Orca orca, GTable gtable, TableMeta table, CursorMaker maker, Operator[] values) {
        super(orca, table, gtable, values);
        this.maker = maker;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        ctx.getSession().lockTable(this.tableId, LockLevel.SHARED, true);
        try (Cursor c = this.maker.make(ctx, params, 0); Heap heap = new FlexibleHeap()) {
            int count = 0;
            for (;;) {
                long pRecord = c.next();
                if (pRecord == 0) {
                    break;
                }
                if (this.ignoreError) {
                    try {
                        insertRow(ctx, heap, null, pRecord);
                        count++;
                    }
                    catch (Exception x) {
                        _log.debug("error from insert is ignored", x);
                        return false;
                    }
                }
                else {
                    insertRow(ctx, heap, null, pRecord);
                    count++;
                }
            }
            return count;
        }
    }
}
