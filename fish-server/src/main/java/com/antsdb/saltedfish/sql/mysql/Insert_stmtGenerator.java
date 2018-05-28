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
import com.antsdb.saltedfish.sql.vdm.Flow;
import com.antsdb.saltedfish.sql.vdm.InsertOnDuplicate;
import com.antsdb.saltedfish.sql.vdm.InsertSelect;
import com.antsdb.saltedfish.sql.vdm.InsertSingleRow;
import com.antsdb.saltedfish.sql.vdm.Instruction;
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
            CursorMaker maker = Select_stmtGenerator.gen(ctx, rule.insert_stmt_select().select_stmt(), null);
            if (maker.getCursorMeta().getColumnCount() != table.getColumns().size()) {
                throw new OrcaException("Column count doesn't match value count");
            }
            GTable gtable = ctx.getGtable(table.getObjectName());
            return new InsertSelect(ctx.getOrca(), gtable, table, maker, isReplace, ignoreError);
        }
        else {
            throw new CodingError();
        }
    }

    private List<InsertSingleRow> gen(GeneratorContext ctx,  TableMeta table,  Insert_stmt_valuesContext rule) {
        // collect column names
        
        List<ColumnMeta> fields = new ArrayList<ColumnMeta>();
        if (rule.insert_stmt_values_columns() != null) {
            for (Column_nameContext i :rule.insert_stmt_values_columns().column_name()) {
                String columnName = Utils.getIdentifier(i.identifier());
                ColumnMeta iii = Checks.columnExist(table, columnName);    
                fields.add(iii);
            }
        }
        else {
            fields.addAll(table.getColumns());
        }
        
        // auto increment column

        List<Operator> defaultValues = new ArrayList<>();
        ColumnMeta autoIncrement = table.findAutoIncrementColumn();
        int posAutoIncrement = -1;
        if (autoIncrement != null) {
            posAutoIncrement = fields.indexOf(autoIncrement);
            if (posAutoIncrement < 0) {
                fields.add(autoIncrement);
                defaultValues.add(new OpIncrementColumnValue(table, null));
            }
        }
        
        // columns with default value
        
        for (ColumnMeta i:table.getColumns()) {
            if (i.getDefault() == null) {
                continue;
            }
            if (fields.contains(i)) {
                continue;
            }
            Operator expr = ExprGenerator.gen(ctx, null, i.getDefault());
            fields.add(i);
            defaultValues.add(expr);
        }
        
        // collect expressions
        
        GTable gtable = ctx.getGtable(table.getObjectName());
        List<InsertSingleRow> result = new ArrayList<>();
        for (Insert_stmt_values_rowContext i: rule.insert_stmt_values_row()) {
            List<Operator> values = new ArrayList<>();
            for (ExprContext j:i.expr()) {
                Operator jj = ExprGenerator.gen(ctx, null, j);
                if (values.size() == posAutoIncrement) {
                    jj = new OpIncrementColumnValue(table, jj);
                }
                values.add(jj);
            }
            values.addAll(defaultValues);
            if (fields.size() != values.size()) {
                throw new OrcaException("number of columns is not matching number of values");
            }

            // auto casting according to column data type
            
            Utils.applyCasting(fields, values);
            
            // end
            
            InsertSingleRow insert = new InsertSingleRow(ctx.getOrca(), table, gtable, fields, values);
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
}
