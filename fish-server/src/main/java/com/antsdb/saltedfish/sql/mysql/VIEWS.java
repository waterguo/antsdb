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

import java.util.Collections;

import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class VIEWS extends CursorMaker {

	Orca orca;
    CursorMeta meta;
    
    public static class Item {
    	public String TABLE_CATALOG;
    	public String TABLE_SCHEMA;
    	public String TABLE_NAME;
    	public String VIEW_DEFINITION;
    	public String CHECK_OPTION;
    	public String IS_UPDATABLE;
    	public String DEFINER;
    	public String SECURITY_TYPE;
    	public String CHARACTER_SET_CLIENT;
    	public String COLLATION_CONNECTION;    
	}
    
    public VIEWS(Orca orca) {
        this.orca = orca;
    	this.meta = CursorUtil.toMeta(Item.class); 
    }
    
	@Override
	public CursorMeta getCursorMeta() {
		return this.meta;
	}

    @Override
	public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Cursor c = CursorUtil.toCursor(meta, Collections.emptyList());
        return c;
    }
}
