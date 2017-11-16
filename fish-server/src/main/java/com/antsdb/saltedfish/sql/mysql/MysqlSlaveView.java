/*-------------------------------------------------------------------------------------------------
 _______ __   _ _______ _______ ______  ______
 |_____| | \  |    |    |______ |     \ |_____]
 |     | |  \_|    |    ______| |_____/ |_____]

 Copyright (c) 2016, antsdb.com and/or its affiliates. All rights reserved. *-xguo0<@

 This program is free software: you can redistribute it and/or modify it under the terms of the
 GNU Affero General Public License, version 3, as published by the Free Software Foundation.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/agpl-3.0.txt>
-------------------------------------------------------------------------------------------------*/
package com.antsdb.saltedfish.sql.mysql;

import java.util.ArrayList;

import com.antsdb.saltedfish.server.mysql.replication.MysqlSlave;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.ViewMaker;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author wgu0
 */
public class MysqlSlaveView extends ViewMaker {

    public static class Item {
        public long TRX_COUNT;
        public long INSERT_COUNT;
        public long UPDATE_COUNT;
        public long DELETE_COUNT;
    }
    
    public MysqlSlaveView() {
        super(CursorUtil.toMeta(Item.class));
    }
    
    @Override
	public Object run(VdmContext ctx, Parameters params, long pMaster) {
        ArrayList<Item> list = new ArrayList<>();
        Item item = new Item();
        item.TRX_COUNT = MysqlSlave._trxCounter;
        item.INSERT_COUNT = MysqlSlave._insertCount;
        item.UPDATE_COUNT = MysqlSlave._updateCount;
        item.DELETE_COUNT = MysqlSlave._deleteCount;
        list.add(item);
        Cursor c = CursorUtil.toCursor(meta, list);
        return c;
	}
}
