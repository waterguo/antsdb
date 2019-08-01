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
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class HSeek extends HSelect {

    private Operator keyExpr;

    public HSeek(GTable gtable, List<Integer> columns) {
        super(gtable, columns);
    }

    public void setKey(Operator op) {
        this.keyExpr = op;
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        BluntHeap heap = null;
        Cursor result = null;
        try {
            heap = new BluntHeap();
            Transaction trx = ctx.getTransaction();
            long pKey = this.keyExpr.eval(ctx, heap, params, pMaster);
            KeyBytes key = KeyBytes.fromHexDump(AutoCaster.getString(heap, pKey));
            long pRow = gtable.get(trx.getTrxId(), trx.getTrxTs(), key.getAddress(), 0);
            if (pRow != 0) {
                long pRecord = Record.alloc(heap, this.meta.getColumnCount());
                Row row = Row.fromMemoryPointer(pRow, 0);
                Record.setKey(pRecord, row.getKeyAddress());
                for (int i=0; i<this.meta.getColumnCount(); i++) {
                    long pValue = row.getFieldAddress(this.mapping[i]);
                    Record.set(pRecord, i, pValue);
                }
                result = CursorUtil.toCursor(meta, heap, pRecord);
            }
            else {
                result = new EmptyCursor(meta);
            }
            heap = null;
        }
        finally {
            if (heap != null) {
                heap.close();
            }
        }
        return wrap(result, params);
    }
    
}
