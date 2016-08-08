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

import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.ForeignKeyMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;

/**
 * 
 * @author *-xguo0<@
 */
public class DropForeignKey extends Statement {
	
	private ObjectName tableName;
	private String name;

	public DropForeignKey(ObjectName tableName, String name) {
		this.tableName = tableName;
		this.name = name;
	}

	@Override
	public Object run(VdmContext ctx, Parameters params) {
		TableMeta table = Checks.tableExist(ctx.getSession(), this.tableName);
		ForeignKeyMeta rule = null;
		for (ForeignKeyMeta i:table.getForeignKeys()) {
			if (i.getName().equalsIgnoreCase(this.name)) {
				rule = i;
				break;
			}
		}
		if (rule == null) {
			throw new OrcaException("foreign key {} is not found", this.name);
		}
		ctx.getMetaService().deleteRule(ctx.getTransaction(), rule);
		return null;
	}

}
