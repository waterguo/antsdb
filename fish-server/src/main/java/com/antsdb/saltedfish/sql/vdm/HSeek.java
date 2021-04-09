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
import com.antsdb.saltedfish.nosql.GetInfo;
import com.antsdb.saltedfish.util.CursorUtil;
import static com.antsdb.saltedfish.nosql.GetOptions.*;

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
            long pKeyString = this.keyExpr.eval(ctx, heap, params, pMaster);
            KeyBytes key = KeyBytes.fromHexDump(AutoCaster.getString(heap, pKeyString));
            long pKey = key.getAddress();
            GetInfo info = new GetInfo();
            long pRow = gtable.getMemTable().get(trx.getTrxId(), trx.getTrxTs(), pKey, NO_CACHE, info);
            if (pRow != 0) {
                long pRecord = Record.alloc(heap, this.meta.getColumnCount());
                Record.copy(heap, info, pRecord, this.mapping);
                result = CursorUtil.toCursor(meta, heap, pRecord);
                heap = null;
            }
            else {
                result = new EmptyCursor(meta);
            }
        }
        finally {
            if (heap != null) {
                heap.close();
            }
        }
        return wrap(result, params);
    }
    
}
