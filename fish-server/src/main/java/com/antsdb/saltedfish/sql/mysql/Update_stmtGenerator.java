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

import com.antsdb.saltedfish.lexer.MysqlParser.Update_stmtContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Update_stmt_setContext;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.GeneratorUtil;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.planner.Planner;
import com.antsdb.saltedfish.sql.vdm.Checks;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.NotNullCheck;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.Operator;

public class Update_stmtGenerator extends Generator<Update_stmtContext> {

    @Override
    public Instruction gen(GeneratorContext ctx, Update_stmtContext rule) throws OrcaException {
        ObjectName tableName = TableName.parse(ctx, rule.table_name_());

        // table cursor
        
        TableMeta table = Checks.tableExist(ctx.getSession(), tableName);
        Planner planner = GeneratorUtil.getSingleTablePlanner(ctx, table);
        
        // apply where clause
        
        if (rule.expr() != null) {
            Operator filter = ExprGenerator.gen(ctx, planner, rule.expr());
            planner.setWhere(filter);
        }
        
        // compile expressions
        
        CursorMaker maker = planner.run();
        Pair<List<ColumnMeta>, List<Operator>> sets = gen(ctx, planner, table, rule.update_stmt_set());
        
        // auto casting according to column data type
        
        Utils.applyCasting(sets.getKey(), sets.getValue());
        
        // done
        if (rule.limit_clause() != null) {
        	    maker = CursorMaker.createLimiter(maker, rule.limit_clause());
        }
        
        return GeneratorUtil.genUpdate(ctx, table, maker, sets.getKey(), sets.getValue());
    }

    static Pair<List<ColumnMeta>, List<Operator>> gen(
            GeneratorContext ctx, 
            Planner planner, 
            TableMeta table,  
            List<? extends Update_stmt_setContext> rules) {
        List<ColumnMeta> columns = new ArrayList<ColumnMeta>();
        List<Operator> exprs = new ArrayList<Operator>();
        for (Update_stmt_setContext i:rules) {
            String columnName = Utils.getIdentifier(i.column_name().identifier());
            ColumnMeta column = table.getColumn(columnName);
            if (column == null) {
                throw new OrcaException("column is not found: " + columnName);
            }
            Operator op = ExprGenerator.gen(ctx, planner, i.expr());
            if (!column.isNullable()) {
                op = new NotNullCheck(op, column);
            }
            columns.add(column);
            exprs.add(op);
        }
        return new Pair<>(columns, exprs);
    }
}
