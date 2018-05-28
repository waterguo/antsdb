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
import java.util.Arrays;

import com.antsdb.saltedfish.lexer.MysqlParser.Show_create_table_stmtContext;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.ForeignKeyMeta;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.PrimaryKeyMeta;
import com.antsdb.saltedfish.sql.meta.RuleMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.ViewMaker;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class Show_create_table_stmtGenerator extends Generator<Show_create_table_stmtContext> {
    public static class Item {
    	    public String Table;
    	    public String Create_Table;
    }

	@Override
	public Instruction gen(GeneratorContext ctx, Show_create_table_stmtContext rule) throws OrcaException {
	    ObjectName tableName = TableName.parse(ctx, rule.table_name_());
	    TableMeta table = ctx.getTable(tableName);
	    if (table == null) {
	        throw new OrcaException("table not found: {}", tableName.toString());
	    }
	    String sql = gen(ctx, table);
		CursorMeta meta = CursorUtil.toMeta(Item.class);
		ArrayList<Item> list = new ArrayList<>();
		Item item = new Item();
		item.Table = tableName.toString();
		item.Create_Table = sql;
		list.add(item);
        return new ViewMaker(meta) {
		    @Override
		    public Object run(VdmContext ctx, Parameters params, long pMaster) {
		        Cursor c = CursorUtil.toCursor(meta, list);
		        return c;
		    }
        };
	}

    private String gen(GeneratorContext ctx, TableMeta table) {
        StringBuilder buf = new StringBuilder();
        buf.append("CREATE TABLE `");
        buf.append(table.getTableName());
        buf.append("` (");
        for (ColumnMeta column:table.getColumns()) {
            buf.append("`");
            buf.append(column.getColumnName());
            buf.append("` ");
            buf.append(column.getDataType().toString());
            buf.append(",");
        }
        appendRule(buf, table, table.getPrimaryKey());
        for (IndexMeta index:table.getIndexes()) {
            appendRule(buf, table, index);
        }
        for (ForeignKeyMeta fk:table.getForeignKeys()) {
            appendForeignKey(ctx.getOrca(), buf, table, fk);
        }
        buf.deleteCharAt(buf.length()-1);
        buf.append(")");
        return buf.toString();
    }

    private void appendForeignKey(Orca orca, StringBuilder buf, TableMeta table, ForeignKeyMeta rule) {
        if (rule == null) {
            return;
        }
        buf.append("FOREIGN KEY ");
        buf.append(rule.getName());
        buf.append(" (");
        for (ColumnMeta column:rule.getColumns(table)) {
            buf.append(column.getColumnName());
            buf.append(",");
        }
        buf.deleteCharAt(buf.length()-1);
        buf.append(") REFERENCES ");
        TableMeta parent = orca.getMetaService().getTable(Transaction.getSeeEverythingTrx(), rule.getParentTable());
        buf.append(parent.getTableName());
        buf.append("(");
        for (int i:rule.getRuleParentColumns()) {
            ColumnMeta column = parent.getColumn(i);
            buf.append(column.getColumnName());
            buf.append(",");
        }
        buf.deleteCharAt(buf.length()-1);
        buf.append("),");
    }

    private void appendRule(StringBuilder buf, TableMeta table, RuleMeta<?> rule) {
        if (rule == null) {
            return;
        }
        if (rule instanceof PrimaryKeyMeta) {
            buf.append("PRIMARY KEY ");
        }
        else if (rule instanceof IndexMeta) {
            if (isForeignKeyIndex(table, (IndexMeta)rule)) {
                return;
            }
            if (((IndexMeta) rule).isUnique()) {
                buf.append("UNIQUE INDEX ");
            }
            else {
                buf.append("INDEX ");
            }
            buf.append(rule.getName());
            buf.append(" ");
        }
        buf.append("(");
        for (ColumnMeta column:rule.getColumns(table)) {
            buf.append(column.getColumnName());
            buf.append(",");
        }
        buf.deleteCharAt(buf.length()-1);
        buf.append(")");
        buf.append(",");
    }

    private boolean isForeignKeyIndex(TableMeta table, IndexMeta rule) {
        if (rule.isUnique()) {
            return false;
        }
        for (ForeignKeyMeta fk:table.getForeignKeys()) {
            if (Arrays.equals(fk.getRuleColumns(), rule.getRuleColumns())) {
                return true;
            }
        }
        return false;
    }

}
