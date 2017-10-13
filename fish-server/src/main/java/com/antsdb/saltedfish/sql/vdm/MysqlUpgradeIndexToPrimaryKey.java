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

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.RuleMeta.Rule;
import com.antsdb.saltedfish.sql.meta.TableMeta;

/**
 * special behavior in mysql. it automatically upgrades unique index to primary key
 *  
 * @author wgu0
 */
public class MysqlUpgradeIndexToPrimaryKey extends Statement {
	ObjectName tableName;
	String indexName;
	
	public MysqlUpgradeIndexToPrimaryKey(ObjectName tableName, String indexName) {
		super();
		this.tableName = tableName;
		this.indexName = indexName;
	}

	@Override
	public Object run(VdmContext ctx, Parameters params) {
		TableMeta table = Checks.tableExist(ctx.getSession(), tableName);
		IndexMeta index = Checks.indexExist(table, this.indexName);
		if (table.getPrimaryKey() != null) {
			return null;
		}
		if (!index.isUnique()) {
			return null;
		}
		for (ColumnMeta i:index.getColumns(table)) {
			if (i.isNullable()) {
				return null;
			}
		}
		GTable gtable = ctx.getHumpback().getTable(table.getHtableId());
		if (!gtable.isPureEmpty()) {
			throw new OrcaException("unable to upgrade unique index to primary key because table is not empty {}", 
					                this.tableName);
		}
		index = index.clone();
		index.setType(Rule.PrimaryKey);
		ctx.getMetaService().updateRule(ctx.getTransaction(), index);
		return null;
	}

}
