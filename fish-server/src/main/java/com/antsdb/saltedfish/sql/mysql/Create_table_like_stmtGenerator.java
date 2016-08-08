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

import com.antsdb.saltedfish.lexer.MysqlParser.Create_table_like_stmtContext;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.Checks;
import com.antsdb.saltedfish.sql.vdm.CreateColumn;
import com.antsdb.saltedfish.sql.vdm.CreateIndex;
import com.antsdb.saltedfish.sql.vdm.CreatePrimaryKey;
import com.antsdb.saltedfish.sql.vdm.CreateTable;
import com.antsdb.saltedfish.sql.vdm.Flow;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.ObjectName;

/**
 * 
 * @author wgu0
 */
public class Create_table_like_stmtGenerator extends Generator<Create_table_like_stmtContext> {

	@Override
	public Instruction gen(GeneratorContext ctx, Create_table_like_stmtContext rule) throws OrcaException {
		ObjectName name = TableName.parse(ctx, rule.table_name_().get(1));
		TableMeta table = Checks.tableExist(ctx.getSession(), name);
		Flow flow = new Flow();
		createTable(ctx, flow, rule, table);
		return flow;
	}

	private void createTable(GeneratorContext ctx, Flow flow, Create_table_like_stmtContext rule, TableMeta table) {
		ObjectName name = TableName.parse(ctx, rule.table_name_().get(0));
		CreateTable ct = new CreateTable(name);
		flow.add(ct);
		createColumns(ctx, flow, table, name);
		createPrimaryKey(ctx, flow, table, name);
		createIndexes(ctx, flow, table, name);
	}

	private void createColumns(GeneratorContext ctx, Flow flow, TableMeta table, ObjectName name) {
		for (ColumnMeta column:table.getColumns()) {
			CreateColumn cc = new CreateColumn();
			flow.add(cc);
			cc.tableName = name;
			cc.setAutoIncrement(column.isAutoIncrement());
			cc.setColumnName(column.getColumnName());
			cc.setDefaultValue(column.getDefault());
			cc.setEnumValues(column.getEnumValues());
			cc.setNullable(column.isNullable());
			cc.setType(column.getDataType());
		}
	}

	private void createPrimaryKey(GeneratorContext ctx, Flow flow, TableMeta table, ObjectName name) {
		if (table.getPrimaryKey() == null) {
			return;
		}
		List<String> columns = new ArrayList<>();
		for (ColumnMeta column:table.getPrimaryKey().getColumns(table)) {
			columns.add(column.getColumnName());
		}
		CreatePrimaryKey cpk = new CreatePrimaryKey(name, columns);
		flow.add(cpk);
	}

	private void createIndexes(GeneratorContext ctx, Flow flow, TableMeta table, ObjectName name) {
		for (IndexMeta index:table.getIndexes()) {
			List<String> columns = new ArrayList<>();
			for (ColumnMeta column:index.getColumns(table)) {
				columns.add(column.getColumnName());
			}
			CreateIndex ci = new CreateIndex(index.getName(), index.isUnique(), false, name, columns);
			flow.add(ci);
		}
	}

}
