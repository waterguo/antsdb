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

import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.util.CursorUtil;

public class SCHEMATA extends CursorMaker {
	Orca orca;
    CursorMeta meta;
    
    public static class Item {
        public String CATALOG_NAME = "def";
        public String SCHEMA_NAME;
        public String DEFAULT_CHARACTER_SET_NAME = "utf8";
        public String DEFAULT_COLLATION_NAME = "utf8_general_ci";
        public String SQL_PATH = null;
    }

    public SCHEMATA(Orca orca) {
        this.orca = orca;
    	this.meta = CursorUtil.toMeta(Item.class); 
    }

	@Override
	public CursorMeta getCursorMeta() {
		return this.meta;
	}
	
    @Override
	public Object run(VdmContext ctx, Parameters params, long pMaster) {
        ArrayList<Item> list = new ArrayList<>();
        ArrayList<String> namespaces = new ArrayList<String>(this.orca.getHumpback().getNamespaces());
        namespaces.add("information_schema");
        for (String i:namespaces) {
            Item item = new Item();
            list.add(item);
            item.SCHEMA_NAME = i;
        }
        Cursor c = CursorUtil.toCursor(meta, list);
        return c;
	}

}
