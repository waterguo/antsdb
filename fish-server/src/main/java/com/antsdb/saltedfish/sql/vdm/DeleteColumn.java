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

import java.util.ArrayList;
import java.util.List;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.PrimaryKeyMeta;
import com.antsdb.saltedfish.sql.meta.RuleMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;

public class DeleteColumn extends Statement {
	ObjectName tableName;
	String columnName;
	
	public DeleteColumn(ObjectName tableName, String columnName) {
		super();
		this.tableName = tableName;
		this.columnName = columnName;
	}

	@Override
	public Object run(VdmContext ctx, Parameters params) {
		TableMeta table = Checks.tableExist(ctx.session, this.tableName);
		ColumnMeta column = Checks.columnExist(table, this.columnName);
		Transaction trx = ctx.getTransaction();

		// scan for affected indexes. 
		
		GTable gtable = ctx.getHumpback().getTable(table.getId());
		for (IndexMeta index:table.getIndexes()) {
			List<ColumnMeta> indexColumns = index.getColumns(table);
			if (indexColumns.contains(column)) {
				if (!gtable.isPureEmpty()) {
					throw new OrcaException("table must be empty in order to drop a index column");
				}
				if (indexColumns.size() == 1) {
					// remove the index
					new DropIndex(this.tableName, index.getName()).run(ctx, params);
				}
				else {
					// remove the index column
					ctx.getMetaService().deleteRuleColumn(trx, index.findRuleColumn(column));
				}
			}
		}
		
		// check primary key 

		if (table.getPrimaryKey() != null) {
			PrimaryKeyMeta pk = table.getPrimaryKey();
			List<ColumnMeta> indexColumns = pk.getColumns(table);
			if (indexColumns.contains(column)) {
				if (!gtable.isPureEmpty()) {
					throw new OrcaException("table must be empty in order to drop a primary key column");
				}
				if (indexColumns.size() == 1) {
					// remove the index
					ctx.getMetaService().deleteRule(trx, pk);
				}
				else {
					// remove the index column
					ctx.getMetaService().deleteRuleColumn(trx, pk.findRuleColumn(column));
				}
			}
		}

		// remove column from meta-data
		
		ctx.getMetaService().deleteColumn(trx, column);
		
		// done
		
		return null;
	}
	
	/**
	 * to be implemented in the future
	 * 
	 * @param ctx
	 * @param params
	 * @return
	 */
	public Object run_(VdmContext ctx, Parameters params) {
		TableMeta table = Checks.tableExist(ctx.session, this.tableName);
		ColumnMeta column = Checks.columnExist(table, this.columnName);
		List<IndexMeta> affected = new ArrayList<>();
		Transaction trx = ctx.getTransaction();
		
		// remove column from meta-data
		
		ctx.getMetaService().deleteColumn(trx, column);
		
		// scan for affected indexes
		
		for (IndexMeta index:table.getIndexes()) {
			List<ColumnMeta> indexColumns = index.getColumns(table);
			if (indexColumns.contains(column)) {
				affected.add(index);
			}
		}
		
		// rebuild everything if primary key affected. otherwise just rebuild affected indexes
		
		if (table.getPrimaryKey() != null) {
			PrimaryKeyMeta pk = table.getPrimaryKey();
			List<String> pkColumns = deleteColumnFromRule(table, pk, column);
			if (pkColumns != null) {
				for (IndexMeta index:affected) {
					ctx.getMetaService().deleteRuleColumn(trx, index.findRuleColumn(column));
				}
				new ModifyPrimaryKey(this.tableName, pkColumns).run(ctx, params, 0);
			}
			else {
				for (IndexMeta index:affected) {
					List<String> indexColumns = deleteColumnFromRule(table, index, column);
					new DropIndex(this.tableName, index.getName()).run(ctx, params);
					new CreateIndex(index.getName(), index.isUnique(), false, this.tableName, indexColumns);
				}
			}
		}
		
		return null;
	}

	List<String> deleteColumnFromRule(TableMeta table, RuleMeta<?> rule, ColumnMeta column) {
		List<String> newColumns = new ArrayList<>();
		List<ColumnMeta> columns = rule.getColumns(table);
		if (!columns.contains(column)) {
			return null;
		}
		for (ColumnMeta i:columns) {
			if (i == column) {
				continue;
			}
			newColumns.add(i.getColumnName());
		}
		return newColumns;
	}
}
