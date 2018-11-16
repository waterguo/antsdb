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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.CommonTokenFactory;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.antlr.v4.runtime.tree.TerminalNodeImpl;
import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.lexer.MysqlParser;
import com.antsdb.saltedfish.lexer.MysqlParser.Column_aliasContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Column_nameContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Column_name_Context;
import com.antsdb.saltedfish.lexer.MysqlParser.ExprContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_functionContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_function_parametersContext;
import com.antsdb.saltedfish.lexer.MysqlParser.From_clauseContext;
import com.antsdb.saltedfish.lexer.MysqlParser.From_clause_odbcContext;
import com.antsdb.saltedfish.lexer.MysqlParser.From_clause_standardContext;
import com.antsdb.saltedfish.lexer.MysqlParser.From_itemContext;
import com.antsdb.saltedfish.lexer.MysqlParser.From_item_odbcContext;
import com.antsdb.saltedfish.lexer.MysqlParser.IdentifierContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Join_itemContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Join_operatorContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Result_columnContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Result_column_exprContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Result_column_starContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Select_or_valuesContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Select_stmtContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Table_name_Context;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.planner.OutputField;
import com.antsdb.saltedfish.sql.planner.Planner;
import com.antsdb.saltedfish.sql.planner.PlannerField;
import com.antsdb.saltedfish.sql.vdm.Checks;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.Dual;
import com.antsdb.saltedfish.sql.vdm.FieldValue;
import com.antsdb.saltedfish.sql.vdm.FuncCount;
import com.antsdb.saltedfish.sql.vdm.FuncMax;
import com.antsdb.saltedfish.sql.vdm.FuncMin;
import com.antsdb.saltedfish.sql.vdm.FuncSum;
import com.antsdb.saltedfish.sql.vdm.Function;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.NullIfEmpty;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.Operator;
import com.antsdb.saltedfish.sql.vdm.ToString;
import com.antsdb.saltedfish.util.CodingError;

public class Select_or_valuesGenerator extends Generator<Select_or_valuesContext>{
    static Set<String> _aggregates = new HashSet<>();
    
    static {
        _aggregates.add("count");
        _aggregates.add("avg");
        _aggregates.add("sum");
        _aggregates.add("min");
        _aggregates.add("max");
    }
    
    @Override
    public Instruction gen(GeneratorContext ctx, Select_or_valuesContext rule)
    throws OrcaException {
        Planner planner = gen(ctx, rule, null);
        CursorMaker maker = planner.run();
        return maker;
    }

    /**
     * generate subquery
     * 
     * @param ctx
     * @param rule
     * @param maker outer query
     * @return
     * @throws OrcaException
     */
    public CursorMaker genSubquery(GeneratorContext ctx, Select_stmtContext rule, Planner parent) 
    throws OrcaException {
        CursorMaker subquery = Select_stmtGenerator.gen(ctx, rule, parent);
        return subquery;
    }
    
    static Planner gen(GeneratorContext ctx, Select_or_valuesContext rule, Planner parent) 
    throws OrcaException {
        Planner planner = new Planner(ctx, parent);
        if (rule.K_SQL_NO_CACHE() != null) {
            planner.setNoCache(true);
        }
        
        genFrom(ctx, rule.from_clause(), planner);
        
        // prevent null cursor. in case of 'select 1'.
        
        if (planner.isEmpty()) {
            planner.addCursor("", new Dual(), true, false); 
        }
        
        // where
        
        if (rule.where_clause() != null) {
            Operator filter = ExprGenerator.gen(ctx, planner, rule.where_clause().expr());
            planner.setWhere(filter);
        }
        
        // final values  
        
        Map<String, Operator> exprByAlias = new HashMap<>();
        for (Result_columnContext it:rule.select_columns().result_column()) {
            ParseTree child = it.getChild(0);
            // process *
            if (child instanceof Result_column_starContext) {
                Result_column_starContext star = (Result_column_starContext)child;
                ObjectName tableAlias = TableName.parse(ctx, star.table_name_());
                for (PlannerField ii:planner.getFields()) {
                    if (tableAlias != null) {
                        if (!tableAlias.getTableName().equalsIgnoreCase(ii.getTableAlias())) {
                            continue;
                        }
                    }
                    if (ii.getColumn() != null) {
                        if (ii.getColumn().getColumnName().startsWith("*")) {
                            continue;
                        }
                    }
                    FieldValue cv = new FieldValue(ii);
                    planner.addOutputField(cv.getName(), cv);
                }
            }
            // expressions
            else if (child instanceof Result_column_exprContext) {
                Result_column_exprContext expr = (Result_column_exprContext)child;
                Operator op = ExprGenerator.gen(ctx, planner, expr.expr());
                // field type cannot be null. if it is null, which means type cannot be predicated, converts it to 
                // varchar. varchar fits all data types
                if (op.getReturnType() == null) {
                    op = new ToString(op);
                }
                String fieldName = getFieldName(expr, op);
                planner.addOutputField(fieldName, op);
                if (expr.column_alias() != null) {
                        exprByAlias.put(fieldName.toLowerCase(), op);
                }
            }
            else {
                throw new CodingError();
            }
        };
        
        // group by, prevent empty cursor if aggregate functions are used
        
        if ((rule.group_by_clause()!=null)) {
            List<Operator> groupbys = new ArrayList<Operator>();
            if (rule.group_by_clause() != null) {
                for (ExprContext i:rule.group_by_clause().expr()) {
                    Operator op = Utils.findInPlannerOutputFields(planner, i.getText());
                    if (op == null) {
                        op = ExprGenerator.gen(ctx, planner, i);
                    }
                    groupbys.add(op);
                }
            }
            planner.setGroupBy(groupbys);
        }
        
        // having clause
        
        if (rule.having_clause() != null) {
            ExprContext rewritten = rewriteHaving(ctx, planner, rule.having_clause().expr());
            Planner newone = new Planner(ctx);
            newone.addCursor("", planner.run(), true, false);
            Operator filter = ExprGenerator.gen(ctx, newone, rewritten);
            newone.setWhere(filter);
            planner = newone;
        }
        
        // distinct
        
        planner.setDistinct(rule.K_DISTINCT() != null);
        
        // done
        
        return planner;
    }
    
    public static void genFrom(GeneratorContext ctx, From_clauseContext rule, Planner planner) {
        if (rule == null) {
            return;
        }
        if (rule.from_clause_standard() != null) {
            genFrom(ctx, rule.from_clause_standard(), planner);
        }
        else {
            genFrom(ctx, rule.from_clause_odbc(), planner);
        }
    }

    private static void genFrom(GeneratorContext ctx, From_clause_odbcContext rule, Planner planner) {
        genFrom(ctx, rule.from_item_odbc(), planner, true, false);
    }

    private static 
    void genFrom(GeneratorContext ctx, From_item_odbcContext rule, Planner planner, boolean left, boolean outer) {
        if (rule.join_operator() != null) {
            genFrom(ctx, rule.from_item_odbc(), planner, left, outer);
            Join_operatorContext joinop = rule.join_operator();
            outer = (joinop.K_LEFT() != null) || (joinop.K_RIGHT() != null);
            left = joinop.K_RIGHT() == null;
            addTableToPlanner(ctx, planner, rule.from_item(), rule.join_constraint().expr(), left, outer);
        }
        else if (rule.from_item() != null) {
            addTableToPlanner(ctx, planner, rule.from_item(), null, left, outer);
        }
        else {
            genFrom(ctx, rule.from_item_odbc(), planner, left, outer);
        }
    }

    private static void genFrom(GeneratorContext ctx, From_clause_standardContext rule, Planner planner) {
        // from items
        
        for (From_itemContext i:rule.from_item()) {
            addTableToPlanner(ctx, planner, i, null, true, false);
        }
        
        // joins
        
        if (rule.join_clause() != null) {
            for (Join_itemContext i:rule.join_clause().join_item()) {
                boolean outer = false;
                boolean left = true;
                if (i.join_operator() != null) {
                    Join_operatorContext joinop = i.join_operator();
                    outer = (joinop.K_LEFT() != null) || (joinop.K_RIGHT() != null);
                    left = joinop.K_RIGHT() == null;
                }
                addTableToPlanner(ctx, planner, i.from_item(), i.join_constraint().expr(), left, outer);
            }
        }
    }

    private static ExprContext rewriteHaving(GeneratorContext ctx, Planner planner, ExprContext expr) {
        ExprContext rewritten = new ExprContext(expr.getParent(), expr.invokingState);
        for (ParseTree i:expr.children) {
            if (i instanceof Expr_functionContext) {
                rewritten.addChild(rewriteHaving(ctx, planner, (Expr_functionContext)i));
            }
            else if (i instanceof ExprContext) {
                rewritten.addChild(rewriteHaving(ctx, planner, (ExprContext)i));
            }
            else if (i instanceof RuleContext) {
                rewritten.addChild((RuleContext)i);
            }
            else if (i instanceof TerminalNode) {
                rewritten.addChild((TerminalNode)i);
            }
            else {
                throw new CodingError();
            }
        }
        return rewritten;
    }

    private static RuleContext rewriteHaving(GeneratorContext ctx, Planner planner, Expr_functionContext rule) {
        String funcname = rule.function_name().getText().toLowerCase();
        if (_aggregates.contains(funcname)) {
            OutputField field = findExisting(ctx, planner, rule);
            if (field != null) {
                return createColumnName_(rule, field);
            }
            else {
                Operator expr = ExprGenerator.gen(ctx, planner, (ExprContext)rule.getParent());
                field = planner.addOutputField("*" + planner.getOutputFields().size(), expr);
                return createColumnName_(rule, field);
            }
        }
        else {
            return rule;
        }
    }

    private static RuleContext createColumnName_(Expr_functionContext rule, OutputField field) {
        Column_name_Context column_name_ = new Column_name_Context(rule.getParent(), rule.invokingState);
        Column_nameContext column_name = new Column_nameContext(column_name_.getParent(), rule.invokingState);
        IdentifierContext identifier = new IdentifierContext(column_name, rule.invokingState);
        CommonToken token = CommonTokenFactory.DEFAULT.create(
                MysqlParser.BACKTICK_QUOTED_IDENTIFIER, 
                '`' + field.name + '`' );
        TerminalNode term = new TerminalNodeImpl(token);
        identifier.addChild(term);
        column_name.addChild(identifier);
        column_name_.addChild(column_name);
        return column_name_;
    }

    private static OutputField findExisting(GeneratorContext ctx, Planner planner, Expr_functionContext rule) {
        for (OutputField i:planner.getOutputFields()) {
            Operator expr = i.getExpr();
            if (!(expr instanceof Function)) {
                continue;
            }
            Function func = (Function)expr;
            String funcname = rule.function_name().getText().toLowerCase();
            if ((func instanceof FuncCount) && funcname.equals("count")) {
                if (matchParameters(ctx, planner, func, rule)) {
                    return i;
                }
            }
            else if ((func instanceof FuncSum) && funcname.equals("sum")) {
                return i;
            }
            else if ((func instanceof FuncMin) && funcname.equals("min")) {
                return i;
            }
            else if ((func instanceof FuncMax) && funcname.equals("max")) {
                return i;
            }
        }
        return null;
    }

    private static boolean matchParameters(
            GeneratorContext ctx, 
            Planner planner, 
            Function func, 
            Expr_functionContext rule) {
        if (rule.expr_function_star_parameter() != null) {
            if (func.getChildren().size() == 1) {
                if (func.getChildren().get(0) == null) {
                    return true;
                }
            }
        }
        Expr_function_parametersContext params = rule.expr_function_parameters();
        if (params == null) {
            return false;
        }
        if (func.getChildren().size() != params.getChildCount()) {
            return false;
        }
        for (int i=0; i<func.getChildren().size(); i++) {
            if (!matchParameter(ctx, planner, func.getChildren().get(i), params.expr(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean matchParameter(GeneratorContext ctx, Planner planner, Operator op, ExprContext rule) {
        if ((op instanceof FieldValue) && (rule.getChild(0) instanceof Column_name_Context)) {
            return matchFieldValue(ctx, planner, (FieldValue)op, (Column_name_Context)rule.getChild(0));
        }
        return false;
    }

    private static boolean matchFieldValue(
            GeneratorContext ctx, 
            Planner planner, 
            FieldValue op, 
            Column_name_Context rule) {
        ObjectName tableName = TableName.parse(ctx, rule.identifier());
        String table = (tableName != null) ? tableName.getTableName() : null;
        String column = Utils.getIdentifier(rule.column_name().identifier());
        PlannerField field = planner.findField(it -> {
            if (!column.equalsIgnoreCase(it.getName())) {
                return false;
            }
            if (table != null) {
                if (!StringUtils.equalsIgnoreCase(table, it.getTableAlias())) {
                    return false;
                }
            }
            return true;
        });
        return field != null;
    }

    static void addTableToPlanner(
            GeneratorContext ctx, 
            Planner planner, 
            From_itemContext rule, 
            ExprContext condition,
            boolean left,
            boolean isOuter) {
        String alias = null;
        ObjectName name = null;
        if (rule.table_alias() != null && rule.table_alias().identifier()!=null) {
            alias = Utils.getIdentifier(rule.table_alias().identifier());
        }
        if (rule.from_subquery() != null) {
            // add sub query to planner
            CursorMaker subquery = (CursorMaker)new Select_or_valuesGenerator().genSubquery(
                    ctx, 
                    rule.from_subquery().select_stmt(),
                    planner);
            name = planner.addCursor(alias, subquery, left, isOuter);
        }
        else if (rule.from_table() != null) {
            Table_name_Context tableName = rule.from_table().table_name_();
            // add table to planner
            Object table = getTable(ctx, tableName);
            name = planner.addTableOrView(alias, table, left, isOuter);
        }
        if (condition != null) {
            Operator expr = ExprGenerator.gen(ctx, planner, condition);
            planner.addJoinCondition(name, expr, left);
        }
    }

    String getAlias(Column_aliasContext any_name) {
        if (any_name.identifier() != null) {
            return Utils.getIdentifier(any_name.identifier());
        }
        else {
            throw new CodingError();
        }
    }
    
    static FieldValue findColumnValue(Operator op) {
        if (op instanceof FieldValue) {
            return (FieldValue)op;
        }
        if (op instanceof NullIfEmpty) {
            return findColumnValue(((NullIfEmpty)op).getInput());
        }
        return null;
    }
    
    static Object getTable(GeneratorContext ctx, Table_name_Context name) {
        ObjectName tableName = TableName.parse(ctx, name);
        if (tableName.getTableName().equalsIgnoreCase("dual")) {
            return new Dual();
        }
        Object table = Checks.tableOrViewExist(ctx.getSession(), tableName);
        return table;
    }

    static String getFieldName(Result_column_exprContext rc, Operator expr) {
        String name;
        if ((rc != null) && (rc.column_alias() != null)) {
            name = Utils.getIdentifier(rc.column_alias().identifier());
        }
        else {
            FieldValue cv = findColumnValue(expr);
            if (cv != null) {
                name = ((FieldValue)expr).getName();
            }
            else {
                name = rc.expr().getText();
            }
        }
        return name;
    }
}
