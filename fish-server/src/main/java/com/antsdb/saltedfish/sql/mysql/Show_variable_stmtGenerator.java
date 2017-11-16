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

import com.antsdb.saltedfish.lexer.MysqlParser.Show_variable_like_clauseContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Show_variable_stmtContext;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.planner.Planner;
import com.antsdb.saltedfish.sql.planner.PlannerField;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;
import com.antsdb.saltedfish.sql.vdm.FieldValue;
import com.antsdb.saltedfish.sql.vdm.Filter;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.OpLike;
import com.antsdb.saltedfish.sql.vdm.Operator;
import com.antsdb.saltedfish.sql.vdm.ShowVariables;
import com.antsdb.saltedfish.sql.vdm.StringLiteral;

public class Show_variable_stmtGenerator extends Generator<Show_variable_stmtContext>{

    @Override
    public Instruction gen(GeneratorContext ctx, Show_variable_stmtContext rule) throws OrcaException {
        CursorMaker maker = new ShowVariables();
        
        if (rule.where_clause() != null) {
        	Planner planner = new Planner(ctx);
        	planner.addCursor("", maker, true, false);
            Operator filter = ExprGenerator.gen(ctx, planner, rule.where_clause().expr());
            maker = new Filter(maker, filter, ctx.getNextMakerId());
        }
        else if (rule.show_variable_like_clause() != null) {
        	Show_variable_like_clauseContext like = rule.show_variable_like_clause();
        	Planner planner = new Planner(ctx);
        	planner.addCursor("", maker, true, false);
        	PlannerField pf = planner.findField((FieldMeta it) -> {
        		return it.getName().equals("Variable_name");
        	});
        	FieldValue fv = new FieldValue(pf);
        	String likeExpr = (Utils.getQuotedLiteralValue(like.literal_value()));
            Operator filter = new OpLike(fv, new StringLiteral(likeExpr));
            maker = new Filter(maker, filter, ctx.getNextMakerId());
        }
        return maker;
    }

}
