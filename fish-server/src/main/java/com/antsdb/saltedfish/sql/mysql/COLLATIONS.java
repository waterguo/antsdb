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
import java.util.List;

import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.ViewMaker;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class COLLATIONS extends ViewMaker {

    Orca orca;

    public static class Item {
        public String COLLATION_NAME;
        public String CHARACTER_SET_NAME;
        public Long ID;
        public String IS_DEFAULT;
        public String IS_COMPILED;
        public Long SORTLEN;

        public Item(String collationName, String charSetName, int id, String isDefault, String isCompiled, int len) {
            this.COLLATION_NAME = collationName;
            this.CHARACTER_SET_NAME = charSetName;
            this.ID = (long) id;
            this.IS_DEFAULT = isDefault;
            this.IS_COMPILED = isCompiled;
            this.SORTLEN = (long) len;
        }
    }

    public COLLATIONS(Orca orca) {
        super(CursorUtil.toMeta(Item.class));
        this.orca = orca;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        List<Item> list = new ArrayList<>();
        Item item = new Item("utf8mb4_general_ci", "utf8mb4", 45, "Yes", "Yes", 1);
        list.add(item);
        item = new Item("utf8mb4_bin", "utf8mb4", 46, "", "Yes", 1);
        list.add(item);
        item = new Item("utf8mb4_unicode_ci", "utf8mb4", 224, "", "Yes", 8);
        list.add(item);
        item = new Item("utf8_bin", "utf8", 83, "", "Yes", 1);
        list.add(item);
        item = new Item("utf8_general_ci", "utf8", 33, "Yes", "Yes", 1);
        list.add(item);
        Cursor c = CursorUtil.toCursor(meta, list);
        return c;
    }
}
