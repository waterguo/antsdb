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

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.antsdb.saltedfish.lexer.MysqlParser.Show_create_table_stmtContext;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.ForeignKeyMeta;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.OrcaTableType;
import com.antsdb.saltedfish.sql.meta.PrimaryKeyMeta;
import com.antsdb.saltedfish.sql.meta.RuleMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;
import com.antsdb.saltedfish.util.MysqlColumnMeta;

/**
 * 
 * @author *-xguo0<@
 */
public class Show_create_table_stmtGenerator extends Generator<Show_create_table_stmtContext> {
    public static class Item {
        @MysqlColumnMeta(column="")
        public String Table;
        @MysqlColumnMeta(alias="Create Table")
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
        item.Table = table.getTableName();
        item.Create_Table = sql;
        list.add(item);
        return new View(meta) {
            @Override
            public Object run(VdmContext ctx, Parameters params, long pMaster) {
                Cursor c = CursorUtil.toCursor(meta, list);
                return c;
            }
        };
    }

    private String gen(GeneratorContext ctx, TableMeta table) {
        StringBuilder buf = new StringBuilder();
        if (table.getType() == OrcaTableType.VIEW) {
            return String.format("CREATE VIEW `%s` AS %s", table.getTableName(), table.getViewSql());
        }
        buf.append("CREATE TABLE `");
        buf.append(table.getTableName());
        buf.append("` (\n");
        for (ColumnMeta column:table.getColumns()) {
            buf.append("  ");
            buf.append("`");
            buf.append(column.getColumnName());
            buf.append("` ");
            if (column.getDataType().getName().equalsIgnoreCase("enum")) {
                buf.append("enum(");
                buf.append(column.getEnumValues());
                buf.append(")");
            }
            else {
                buf.append(column.getDataType().toString());
            }
            if (!column.isNullable()) {
                buf.append(" NOT NULL");
            }
            if (column.getDefault() != null) {
                buf.append(" DEFAULT ");
                buf.append(column.getDefault());
            }
            else if (column.isNullable()) {
                // fucking weird, TEXT and BLOB types doesn't need to append NULL
                int sqlType = column.getDataType().getSqlType();
                if ((sqlType != Types.CLOB) && (sqlType != Types.BLOB)) {
                    buf.append(" DEFAULT NULL");
                }
            }
            if (column.isAutoIncrement()) {
                buf.append(" AUTO_INCREMENT");
            }
            buf.append(",\n");
        }
        appendRule(buf, table, table.getPrimaryKey());
        for (IndexMeta index:table.getIndexes()) {
            appendRule(buf, table, index);
        }
        for (ForeignKeyMeta fk:table.getForeignKeys()) {
            appendForeignKey(ctx.getOrca(), buf, table, fk);
        }
        buf.deleteCharAt(buf.length()-1);
        buf.deleteCharAt(buf.length()-1);
        buf.append("\n) ENGINE=");
        String engine = table.getEngine();
        buf.append(engine != null ? engine : "InnoDB");
        buf.append(" DEFAULT CHARSET=");
        buf.append(table.getCharset() != null ? table.getCharset() : "utf8");
        return buf.toString();
    }

    private void appendForeignKey(Orca orca, StringBuilder buf, TableMeta table, ForeignKeyMeta rule) {
        if (rule == null) {
            return;
        }
        buf.append("  CONSTRAINT");
        buf.append(" `");
        buf.append(rule.getName());
        buf.append("` ");
        buf.append("FOREIGN KEY ");
        buf.append("(");
        for (ColumnMeta column:rule.getColumns(table)) {
            buf.append("`");
            buf.append(column.getColumnName());
            buf.append("`");
            buf.append(",");
        }
        buf.deleteCharAt(buf.length()-1);
        buf.append(") REFERENCES ");
        buf.append("`");
        buf.append(rule.getParentTable().getTableName());
        buf.append("`");
        buf.append(" (");
        for (String i:rule.getParentColumns()) {
            buf.append("`");
            buf.append(i);
            buf.append("`");
            buf.append(",");
        }
        buf.deleteCharAt(buf.length()-1);
        buf.append(")");
        if (rule.getOnDelete() != null) {
            buf.append(" ON DELETE ");
            buf.append(rule.getOnDelete());
        }
        buf.append(",\n");
    }

    private void appendRule(StringBuilder buf, TableMeta table, RuleMeta<?> rule) {
        if (rule == null) {
            return;
        }
        else if (rule instanceof IndexMeta) {
            appendIndex(buf, table, (IndexMeta)rule);
            return;
        }
        if (rule instanceof PrimaryKeyMeta) {
            buf.append("  ");
            buf.append("PRIMARY KEY ");
        }
        buf.append("(");
        for (ColumnMeta column:rule.getColumns(table)) {
            buf.append("`");
            buf.append(column.getColumnName());
            buf.append("`");
            buf.append(",");
        }
        buf.deleteCharAt(buf.length()-1);
        buf.append(")");
        buf.append(",\n");
    }

    private void appendIndex(StringBuilder buf, TableMeta table, IndexMeta rule) {
        if (isForeignKeyIndex(table, (IndexMeta)rule)) {
            return;
        }
        buf.append("  ");
        if (rule.isUnique()) {
            buf.append("UNIQUE KEY ");
        }
        else if (rule.isFullText()) {
            buf.append("FULLTEXT KEY ");
        }
        else {
            buf.append("KEY ");
        }
        buf.append("`");
        buf.append(rule.getName());
        buf.append("`");
        buf.append(" ");
        buf.append("(");
        List<Integer> prefixes = rule.getPrefix();
        List<ColumnMeta> columns = rule.getColumns(table);
        for (int i=0; i<columns.size(); i++) {
            buf.append("`");
            buf.append(columns.get(i).getColumnName());
            buf.append("`");
            if ((prefixes != null) && (i<prefixes.size())) {
                Integer prefix = prefixes.get(i);
                if (prefix != null) {
                    buf.append("(");
                    buf.append(prefix.toString());
                    buf.append(")");
                }
            }
            buf.append(",");
        }
        buf.deleteCharAt(buf.length()-1);
        buf.append(")");
        buf.append(",\n");
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
