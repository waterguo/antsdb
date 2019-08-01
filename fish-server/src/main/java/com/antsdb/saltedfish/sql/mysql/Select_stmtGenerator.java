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
        return gen(ctx, rule, null);
    }

    public static CursorMaker gen(GeneratorContext ctx, Select_stmtContext rule, Planner parent) {
        CursorMaker maker;

        // this is a temporary solution until there is a better planner
        if (rule.select_or_values().size() == 1) {
            maker = genWithoutUnion(ctx, rule, parent);
        }
        else {
            maker = genWithUnion(ctx, rule, parent);
        }
        if (rule.limit_clause() != null) {
            maker = CursorMaker.createLimiter(maker, rule.limit_clause());
        }
        return maker;
    }
    
    private static CursorMaker genWithUnion(GeneratorContext ctx, Select_stmtContext rule, Planner parent) {
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
        
        // order by
        
        if (rule.order_by_clause() != null) {
            Planner planner = new Planner(ctx);
            planner.addCursor("", maker, true, false);
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
        
       return maker;
    }

    private static CursorMaker genWithoutUnion(GeneratorContext ctx, Select_stmtContext rule, Planner parent) {
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
        
        return planner.run();
    }

    private static CursorMaker genMaker(GeneratorContext ctx, Select_or_valuesContext rule, Planner parent) {
        Planner planner = Select_or_valuesGenerator.gen(ctx, rule, parent);
        return planner.run();
    }

}
