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

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.cpp.Unicode16;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.GetInfo;
import static com.antsdb.saltedfish.nosql.ScanOptions.*;

/**
 * 
 * @author *-xguo0<@
 */
public class HIndexSeek extends HSelectIndex {

    private Operator key;

    private class MyCursor extends CursorWithHeap {
        long pRecord;
        
        public MyCursor(CursorMeta meta) {
            super(meta);
        }

        @Override
        public long next() {
            long pResult = this.pRecord;
            this.pRecord = 0;
            return pResult;
        }
    }
    
    public HIndexSeek(GTable gindex) {
        super(gindex);
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Transaction trx = ctx.getTransaction();
        try (BluntHeap heap = new BluntHeap()) {
            long pKey = this.key.eval(ctx, heap, params, pMaster);
            KeyBytes key = KeyBytes.fromHexDump(AutoCaster.getString(heap, pKey));
            long pIndexKey = key.getAddress();
            GetInfo info = new GetInfo();
            long pIndex = gindex.getMemTable().getIndex(trx.getTrxId(), trx.getTrxTs(), pIndexKey, NO_CACHE, info);
            if (pIndex == 0) {
                return new EmptyCursor(getCursorMeta());
            }
            MyCursor result = new MyCursor(getCursorMeta());
            result.pRecord = result.newRecord();
            KeyBytes indexKey = KeyBytes.allocSet(result.getHeap(), new KeyBytes(pIndexKey));
            KeyBytes rowKey = KeyBytes.allocSet(result.getHeap(), new KeyBytes(pIndex));
            Record.setKey(result.pRecord, indexKey.getAddress());
            Record.set(result.pRecord, 0, indexKey.getAddress());
            Record.set(result.pRecord, 1, Unicode16.allocSet(result.getHeap(), info.location));
            Record.set(result.pRecord, 2, rowKey.getAddress());
            Record.set(result.pRecord, 3, rowKey.getSuffixByte());
            return wrap(result, params);
        }
    }

    public void setKey(Operator key) {
        this.key = key;
    }
}
