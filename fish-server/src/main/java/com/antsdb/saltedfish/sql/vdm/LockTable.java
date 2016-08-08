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
package com.antsdb.saltedfish.sql.vdm;

import java.util.List;

import com.antsdb.saltedfish.sql.LockLevel;
import com.antsdb.saltedfish.sql.meta.TableMeta;

/**
 * 
 * @author wgu0
 */
public class LockTable extends Statement {
	List<ObjectName> tableNames;
    
    public LockTable(List<ObjectName> tables) {
    	this.tableNames = tables;
    }

	@Override
	public Object run(VdmContext ctx, Parameters params) {
		for (ObjectName i:this.tableNames) {
			TableMeta table = Checks.tableExist(ctx.getSession(), i);
			ctx.getSession().lockTable(table.getId(), LockLevel.EXCLUSIVE, false);
		}
		return null;
	}
}
