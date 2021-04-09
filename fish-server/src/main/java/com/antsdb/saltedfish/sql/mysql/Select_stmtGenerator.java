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

import org.apache.commons.lang.NotImplementedException;

import com.antsdb.saltedfish.lexer.MysqlParser.Compound_operatorContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Limit_clauseContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Ordering_termContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Select_or_valuesContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Select_stmtContext;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.planner.Planner;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.Operator;
import com.antsdb.saltedfish.sql.vdm.Union;

public class Select_stmtGenerator extends Generator<Select_stmtContext>{

    @Override
    public Instruction gen(GeneratorContext ctx, Select_stmtContext rule)
    throws OrcaException {
        return gen(ctx, rule, null).run();
    }

    public static Planner gen(GeneratorContext ctx, Select_stmtContext rule, Planner parent) {
        Planner maker;

        // this is a temporary solution until there is a better planner
        if (rule.select_or_values().size() == 1) {
            maker = genWithoutUnion(ctx, rule, parent);
        }
        else {
            maker = genWithUnion(ctx, rule, parent);
        }
        if (rule.limit_clause() != null) {
            setLimit(maker, rule.limit_clause());
        }
        return maker;
    }
    
    static void setLimit(Planner planner, Limit_clauseContext rule) {
        long offset = 0;
        long count = Long.parseLong(rule.number_value(0).getText());
        if (rule.K_OFFSET() != null) {
            offset = Long.parseLong(rule.number_value(1).getText());
        }
        else {
            if (rule.number_value(1) != null) {
                offset = count;
                count = Long.parseLong(rule.number_value(1).getText());
            }
        }
        planner.setLimit(offset, count);
    }

    private static Planner genWithUnion(GeneratorContext ctx, Select_stmtContext rule, Planner parent) {
        CursorMaker maker = genMaker(ctx, rule.select_or_values(0), parent);
        for (int i=1; i<rule.getChildCount(); i++) {
            if (rule.getChild(i) instanceof Compound_operatorContext) {
                Compound_operatorContext ii = (Compound_operatorContext)rule.getChild(i);
                if (ii.K_UNION() != null) {
                    Select_or_valuesContext select = (Select_or_valuesContext)rule.getChild(i+1);
                    maker = new Union(maker, genMaker(ctx, select, parent), ii.K_ALL() != null, ctx.getNextMakerId());
                }
                else {
                    throw new NotImplementedException();
                }
            }
        }
        
        // fields
        Planner planner = new Planner(ctx);
        planner.addCursor("", maker, true, false);
        planner.addAllFields();
        
        // order by
        if (rule.order_by_clause() != null) {
            List<Operator> orderExprs = new ArrayList<Operator>();
            List<Boolean> directions = new ArrayList<Boolean>();
            for (Ordering_termContext i:rule.order_by_clause().ordering_term()) {
                Operator op = Utils.findInPlannerOutputFieldsForOrderBy(planner, i.expr().getText());
                if (op == null) {
                    op = ExprGenerator.gen(ctx, planner, i.expr());
                }
                boolean direction = (i.K_DESC() == null) ? true : false;
                orderExprs.add(op);
                directions.add(direction);
            }
            planner.setOrderBy(orderExprs, directions);

            // for update
            if (rule.K_UPDATE() != null) {
                planner.setForUpdate(true);
            }
            
            maker = planner.run();
        }
        
       return planner;
    }

    private static Planner genWithoutUnion(GeneratorContext ctx, Select_stmtContext rule, Planner parent) {
        Planner planner = Select_or_valuesGenerator.gen(ctx, rule.select_or_values(0), parent);

        // order by
        if (rule.order_by_clause() != null) {
            List<Operator> orderExprs = new ArrayList<Operator>();
            List<Boolean> directions = new ArrayList<Boolean>();
            for (Ordering_termContext i:rule.order_by_clause().ordering_term()) {
                Operator op = Utils.findInPlannerOutputFieldsForOrderBy(planner, i.expr().getText());
                if (op == null) {
                    op = ExprGenerator.gen(ctx, planner, i.expr());
                }
                boolean direction = (i.K_DESC() == null) ? true : false;
                orderExprs.add(op);
                directions.add(direction);
            }
            planner.setOrderBy(orderExprs, directions);
        }
        
        // for update
        if (rule.K_UPDATE() != null) {
            planner.setForUpdate(true);
        }
        
        return planner;
    }

    private static CursorMaker genMaker(GeneratorContext ctx, Select_or_valuesContext rule, Planner parent) {
        Planner planner = Select_or_valuesGenerator.gen(ctx, rule, parent);
        return planner.run();
    }

}
