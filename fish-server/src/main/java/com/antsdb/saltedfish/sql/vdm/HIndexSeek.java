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

import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.nosql.GTable;

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

        void set(long pIndexKey, long pRowKey) {
            this.pRecord = newRecord();
            KeyBytes indexKey = KeyBytes.allocSet(getHeap(), new KeyBytes(pIndexKey));
            KeyBytes rowKey = KeyBytes.allocSet(getHeap(), new KeyBytes(pRowKey));
            Record.setKey(pRecord, indexKey.getAddress());
            Record.set(pRecord, 0, indexKey.getAddress());
            Record.set(pRecord, 1, rowKey.getAddress());
            Record.set(pRecord, 2, rowKey.getSuffixByte());
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
        MyCursor result = new MyCursor(getCursorMeta());
        Transaction trx = ctx.getTransaction();
        long pKey = this.key.eval(ctx, result.getHeap(), params, pMaster);
        KeyBytes key = KeyBytes.fromHexDump(AutoCaster.getString(result.getHeap(), pKey));
        long pIndexKey = key.getAddress();
        long pRowKey = gindex.getIndex(trx.getTrxId(), trx.getTrxTs(), pIndexKey);
        if (pRowKey != 0) {
            result.set(pIndexKey, pRowKey);
        }
        return wrap(result, params);
    }

    public void setKey(Operator key) {
        this.key = key;
    }
}
