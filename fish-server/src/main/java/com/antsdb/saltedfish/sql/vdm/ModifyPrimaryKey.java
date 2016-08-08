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

import org.apache.commons.lang.NotImplementedException;

import com.antsdb.saltedfish.sql.LockLevel;
import com.antsdb.saltedfish.sql.meta.TableMeta;

/**
 * change the primary key of a table
 *  
 * @author wgu0
 */
public class ModifyPrimaryKey extends Statement {
	ObjectName tableName;
	List<String> columns;
	
	public ModifyPrimaryKey(ObjectName tableName, List<String> columns) {
		super();
		this.tableName = tableName;
		this.columns = columns;
	}

	@Override
	public Object run(VdmContext ctx, Parameters params) {
		TableMeta table = Checks.tableExist(ctx.getSession(), this.tableName);
		try {
			ctx.getSession().lockTable(table.getId(), LockLevel.EXCLUSIVE, false);
	        // copy the table over to a new one
	        
	        ObjectName newName = ctx.getMetaService().findUniqueName(ctx.getTransaction(), this.tableName);
	        new CreateTable(newName).run(ctx, params);
	        
	        // ..... more more to be implemented
	        
	        throw new NotImplementedException();
		}
		finally {
			ctx.getSession().unlockTable(table.getId());
		}
	}

}
