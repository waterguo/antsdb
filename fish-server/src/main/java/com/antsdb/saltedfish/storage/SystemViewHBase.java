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
package com.antsdb.saltedfish.storage;

import java.util.ArrayList;

import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.EmptyCursor;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author wgu0
 */
public class SystemViewHBase extends View {
    Orca orca;
    
    public static class Item {
        public long TRX_COUNT;
        public long ROW_COUNT;
        public long INSERTUPDATE_COUNT;
        public long DELETE_COUNT;
        public long INDEX_COUNT;
        public long SP;
    }

    public SystemViewHBase(Orca orca) {
        super(CursorUtil.toMeta(Item.class));
        this.orca = orca;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        HBaseReplicationHandler handler = getHandler();
        if (handler == null) {
            return new EmptyCursor(this.meta);
        }
        HBaseStorageService service = getService();
        ArrayList<Item> list = new ArrayList<>();
        if (handler != null) {
            Item item = new Item();
            list.add(item);
            item.TRX_COUNT = handler.trxCount;
            item.ROW_COUNT = handler.rowCount;
            item.INSERTUPDATE_COUNT = handler.putRowCount;
            item.DELETE_COUNT = handler.deleteRowCount;
            item.INDEX_COUNT = handler.indexRowCount;
            item.SP = service.getCurrentSP();
        }
        Cursor c = CursorUtil.toCursor(meta, list);
        return c;
    }

    HBaseStorageService getService() {
        HBaseStorageService hbase = this.orca.getHBaseStorageService();
        return hbase;
    }
    
    HBaseReplicationHandler getHandler() {
        if (getService() == null) {
            return null;
        }
        HBaseReplicationHandler result = (HBaseReplicationHandler)getService().getReplicable().getReplayHandler();
        return result;
    }
}
