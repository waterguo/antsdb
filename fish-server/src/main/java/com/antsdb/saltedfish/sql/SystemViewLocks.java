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
package com.antsdb.saltedfish.sql;

import java.util.ArrayList;
import java.util.List;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author wgu0
 */
public class SystemViewLocks extends View {
    Orca orca;
    
    public static class Item {
        public int SESSION;
        public String LOCK_TYPE;
        public Integer OBJECT_ID;
        public String OBJECT;
    }
    
    public SystemViewLocks(Orca orca) {
        super(CursorUtil.toMeta(Item.class));
        this.orca = orca;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        List<Item> list = new ArrayList<>();
        for (Session session:ctx.getOrca().getSessions()) {
            for (TableLock lock:session.tableLocks.values()) {
                if (lock.getLevel() == LockLevel.EXCLUSIVE_BY_OTHER) {
                    continue;
                }
                if (lock.getLevel() == LockLevel.NONE) {
                    continue;
                }
                list.add(add(ctx, lock));
            }
        }

        // done
        
        Cursor c = CursorUtil.toCursor(meta, list);
        return c;
    }

    private Item add(VdmContext ctx, TableLock lock) {
        GTable table = ctx.getHumpback().getTable(lock.tableId);
        Item item = new Item();
        item.SESSION = lock.owner;
        item.LOCK_TYPE = LockLevel.toString(lock.level.get());
        item.OBJECT_ID = table.getId();
        item.OBJECT = table.getNamespace() + "." + table.getName();
        return item;
    }
}
