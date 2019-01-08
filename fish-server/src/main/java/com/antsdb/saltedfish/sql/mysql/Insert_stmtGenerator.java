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
import java.util.List;

import org.apache.commons.math3.util.Pair;

import com.antsdb.saltedfish.lexer.MysqlParser.*;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.GeneratorUtil;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.planner.Planner;
import com.antsdb.saltedfish.sql.vdm.Checks;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;
import com.antsdb.saltedfish.sql.vdm.FieldValue;
import com.antsdb.saltedfish.sql.vdm.Flow;
import com.antsdb.saltedfish.sql.vdm.InsertOnDuplicate;
import com.antsdb.saltedfish.sql.vdm.InsertSelect;
import com.antsdb.saltedfish.sql.vdm.InsertSingleRow;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.NotNullCheck;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.OpIncrementColumnValue;
import com.antsdb.saltedfish.sql.vdm.Operator;
import com.antsdb.saltedfish.util.CodingError;

public class Insert_stmtGenerator extends Generator<Insert_stmtContext> {

    @Override
    public Instruction gen(GeneratorContext ctx, Insert_stmtContext rule)
    throws OrcaException {
        ObjectName name = TableName.parse(ctx, rule.table_name_());
        TableMeta table = Checks.tableExist(ctx.getSession(), name);
        boolean isReplace = rule.K_REPLACE() != null;
        boolean ignoreError = rule.K_IGNORE() != null;
        if (rule.insert_stmt_values() != null) {
            List<InsertSingleRow> inserts = gen(ctx, table, rule.insert_stmt_values());
            for (InsertSingleRow i:inserts) {
                i.setReplace(isReplace);
                i.setIgnoreError(ignoreError);
            }
            if (rule.insert_duplicate_clause() == null) {
                return new Flow(inserts);
            }
            else {
                return new Flow(gen(ctx, table, inserts, rule.insert_duplicate_clause()));
            }
        }
        else if (rule.insert_stmt_select() != null) {
            InsertSelect result = gen(ctx, table, rule.insert_stmt_select());
            result.setIgnoreError(ignoreError);
            result.setReplace(isReplace);
            return result;
        }
        else {
            throw new CodingError();
        }
    }

    private InsertSelect gen(GeneratorContext ctx,  TableMeta table,  Insert_stmt_selectContext rule) {
        CursorMaker maker = Select_stmtGenerator.gen(ctx, rule.select_stmt(), null);
        GTable gtable = ctx.getGtable(table.getObjectName());
        List<ColumnMeta> columns = genColumns(table, rule.insert_stmt_values_columns());
        Operator[] values = new Operator[table.getMaxColumnId() + 1];
        List<FieldMeta> fields = maker.getCursorMeta().getColumns();
        if (rule.insert_stmt_values_columns() != null) {
            if (fields.size() != rule.insert_stmt_values_columns().column_name().size()) {
                throw new OrcaException("number of columns is not matching number of values");
            }
        }
        for (int i=0; i<maker.getCursorMeta().getColumnCount(); i++) {
            FieldMeta ii = maker.getCursorMeta().getColumn(i);
            Operator value = new FieldValue(ii, i);
            ColumnMeta column = columns.get(i);
            value = Utils.autoCast(column, value);
            values[column.getColumnId()] = value;
        }
        setDefaultValues(ctx, table, values);
        return new InsertSelect(ctx.getOrca(), gtable, table, maker, values);
    }
    
    private List<InsertSingleRow> gen(GeneratorContext ctx,  TableMeta table,  Insert_stmt_valuesContext rule) {
        List<InsertSingleRow> result = new ArrayList<>();
        List<ColumnMeta> columns = genColumns(table, rule.insert_stmt_values_columns());
        GTable gtable = ctx.getGtable(table.getObjectName());
        for (Insert_stmt_values_rowContext i: rule.insert_stmt_values_row()) {
            Operator[] values = new Operator[table.getMaxColumnId() + 1];
            List<? extends ExprContext> exprs = i.expr();
            if (exprs.size() != columns.size()) {
                throw new OrcaException("column count doesn't match value count");
            }
            for (int j=0; j<exprs.size(); j++) {
                ExprContext expr = exprs.get(j);
                ColumnMeta column = columns.get(j);
                Operator jj = ExprGenerator.gen(ctx, null, expr);
                jj = Utils.autoCast(column, jj);
                values[column.getColumnId()] = jj;
            }
            setDefaultValues(ctx, table, values);
            InsertSingleRow insert = new InsertSingleRow(ctx.getOrca(), table, gtable, values);
            result.add(insert);
        }
        return result;
    }

    private List<InsertOnDuplicate> gen(GeneratorContext ctx, 
                                        TableMeta table, 
                                        List<InsertSingleRow> inserts, 
                                        Insert_duplicate_clauseContext rule) {
        List<InsertOnDuplicate> result = new ArrayList<>();
        Planner planner = GeneratorUtil.getSingleTablePlanner(ctx, table);
        Pair<List<ColumnMeta>, List<Operator>> sets;
        GTable gtable = ctx.getGtable(table.getObjectName());
        sets = Update_stmtGenerator.gen(ctx, planner, table, rule.update_stmt_set());
        planner.run();
        for (InsertSingleRow insert:inserts) {
            InsertOnDuplicate iod = new InsertOnDuplicate(ctx.getOrca(), table, gtable, sets.getKey(), sets.getValue());
            iod.setInsert(insert);
            result.add(iod);
        }
        return result;
    }
    
    private List<ColumnMeta> genColumns(TableMeta table, Insert_stmt_values_columnsContext rule) {
        List<ColumnMeta> fields = new ArrayList<ColumnMeta>();
        if (rule != null) {
            for (Column_nameContext i :rule.column_name()) {
                String columnName = Utils.getIdentifier(i.identifier());
                ColumnMeta iii = Checks.columnExist(table, columnName);    
                fields.add(iii);
            }
        }
        else {
            fields.addAll(table.getColumns());
        }
        return fields;
    }

    private void setDefaultValues(GeneratorContext ctx, TableMeta table, Operator[] values) {
        List<ColumnMeta> columns = table.getColumns();
        for (ColumnMeta i:columns) {
            if (values[i.getColumnId()] != null) {
                if (i.isAutoIncrement()) {
                    values[i.getColumnId()] = new OpIncrementColumnValue(table, values[i.getColumnId()]);
                }
            }
            else if (i.getDefault() != null) {
                Operator value = ExprGenerator.gen(ctx, null, i.getDefault());
                value = Utils.autoCast(i, value);
                values[i.getColumnId()] = value;
            }
            else if (i.isAutoIncrement()) {
                values[i.getColumnId()] = new OpIncrementColumnValue(table, null);
            }
            if (!i.isNullable()) {
                values[i.getColumnId()] = new NotNullCheck(values[i.getColumnId()], i);
            }
        }
    }
    
}
