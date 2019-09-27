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

import com.antsdb.saltedfish.lexer.MysqlParser.Delete_stmtContext;
import com.antsdb.saltedfish.lexer.MysqlParser.From_clause_standardContext;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.GeneratorUtil;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.planner.Planner;
import com.antsdb.saltedfish.sql.planner.PlannerField;
import com.antsdb.saltedfish.sql.vdm.Checks;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;
import com.antsdb.saltedfish.sql.vdm.FieldValue;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.Operator;

public class Delete_stmtGenerator extends Generator<Delete_stmtContext>{

    @Override
    public Instruction gen(GeneratorContext ctx, Delete_stmtContext rule) throws OrcaException {
        Planner planner = new Planner(ctx);

        Select_or_valuesGenerator.genFrom(ctx, rule.from_clause(), planner);
        
        // find the table to work on
        
        TableMeta table = null;
        if (rule.table_name_() != null) {
            String name = rule.table_name_().getText();
            Object obj = planner.findTable(name);
            if (obj == null) {
                throw new OrcaException("table is not found {}", rule.table_name_().getText());
            }
            if (!(obj instanceof TableMeta)) {
                throw new OrcaException("{} must be a table", rule.table_name_().getText());
            }
            table = (TableMeta)obj;
            PlannerField field = planner.findField((FieldMeta it) -> {
                if (it.getColumn() == null) {
                    return false;
                }
                if (it.getColumn().getColumnId() != -1) {
                    return false;
                }
                return (it.getTableAlias().equals(name));
            });
            planner.addOutputField(name, new FieldValue(field));
        }
        else {
            From_clause_standardContext fromStandard = rule.from_clause().from_clause_standard();
            ObjectName name = TableName.parse(ctx, fromStandard.from_item(0).from_table().table_name_());
            table = Checks.tableExist(ctx.getSession(), name);
        }
        
        // apply where clause
        
        if (rule.expr() != null) {
            Operator filter = ExprGenerator.gen(ctx, planner, rule.expr());
            planner.setWhere(filter);
        }
        
        // done
        if (rule.limit_clause() != null) {
            Select_stmtGenerator.setLimit(planner, rule.limit_clause());
        }
        return GeneratorUtil.genDelete(ctx, table, planner.run());
    }

}
