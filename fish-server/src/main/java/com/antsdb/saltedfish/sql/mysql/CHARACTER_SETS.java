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
package com.antsdb.saltedfish.sql.mysql;

import java.util.ArrayList;
import java.util.List;

import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class CHARACTER_SETS extends View {

    Orca orca;

    public static class Item {
        public Item(String name, String collation, String desc, int len) {
            CHARACTER_SET_NAME = name;
            DEFAULT_COLLATE_NAME = collation;
            DESCRIPTION = desc;
            MAXLEN = (long) len;
        }

        public String CHARACTER_SET_NAME;
        public String DEFAULT_COLLATE_NAME;
        public String DESCRIPTION;
        public Long MAXLEN;
    }

    public CHARACTER_SETS(Orca orca) {
        super(CursorUtil.toMeta(Item.class));
        this.orca = orca;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        List<Item> list = new ArrayList<>();
        Item item = new Item("utf8mb4", "utf8mb4_general_ci", "UTF-8 Unicode", 4);
        list.add(item);
        item = new Item("utf8", "utf8_general_ci", "UTF-8 Unicode", 3);
        list.add(item);
        Cursor c = CursorUtil.toCursor(meta, list);
        return c;
    }
}
