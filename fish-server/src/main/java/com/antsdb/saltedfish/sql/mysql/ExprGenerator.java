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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.lexer.MysqlLexer;
import com.antsdb.saltedfish.lexer.MysqlParser;
import com.antsdb.saltedfish.lexer.MysqlParser.Bind_parameterContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Column_name_Context;
import com.antsdb.saltedfish.lexer.MysqlParser.ExprContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_caseContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_case_whenContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_castContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_existContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_functionContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_in_selectContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_in_valuesContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_matchContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_notContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_parenthesisContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_selectContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_simpleContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Group_concat_parameterContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Like_exprContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Literal_intervalContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Literal_valueContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Literal_value_binaryContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Session_variable_referenceContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Unary_operatorContext;
import com.antsdb.saltedfish.lexer.MysqlParser.ValueContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Variable_referenceContext;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.planner.Planner;
import com.antsdb.saltedfish.sql.planner.PlannerField;
import com.antsdb.saltedfish.sql.vdm.BindParameter;
import com.antsdb.saltedfish.sql.vdm.BytesValue;
import com.antsdb.saltedfish.sql.vdm.CurrentTime;
import com.antsdb.saltedfish.sql.vdm.CurrentTimestamp;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.FieldValue;
import com.antsdb.saltedfish.sql.vdm.FuncAbs;
import com.antsdb.saltedfish.sql.vdm.FuncAvg;
import com.antsdb.saltedfish.sql.vdm.FuncBase64;
import com.antsdb.saltedfish.sql.vdm.FuncCast;
import com.antsdb.saltedfish.sql.vdm.FuncConcat;
import com.antsdb.saltedfish.sql.vdm.FuncConnectionId;
import com.antsdb.saltedfish.sql.vdm.FuncCount;
import com.antsdb.saltedfish.sql.vdm.FuncCurDate;
import com.antsdb.saltedfish.sql.vdm.FuncCurrentUser;
import com.antsdb.saltedfish.sql.vdm.FuncDatabase;
import com.antsdb.saltedfish.sql.vdm.FuncDateAdd;
import com.antsdb.saltedfish.sql.vdm.FuncDateFormat;
import com.antsdb.saltedfish.sql.vdm.FuncDateSub;
import com.antsdb.saltedfish.sql.vdm.FuncDistinct;
import com.antsdb.saltedfish.sql.vdm.FuncElt;
import com.antsdb.saltedfish.sql.vdm.FuncEmptyClob;
import com.antsdb.saltedfish.sql.vdm.FuncField;
import com.antsdb.saltedfish.sql.vdm.FuncFindInSet;
import com.antsdb.saltedfish.sql.vdm.FuncFromUnixTime;
import com.antsdb.saltedfish.sql.vdm.FuncGetLock;
import com.antsdb.saltedfish.sql.vdm.FuncGroupConcat;
import com.antsdb.saltedfish.sql.vdm.FuncHex;
import com.antsdb.saltedfish.sql.vdm.FuncIf;
import com.antsdb.saltedfish.sql.vdm.FuncLeft;
import com.antsdb.saltedfish.sql.vdm.FuncLength;
import com.antsdb.saltedfish.sql.vdm.FuncLocate;
import com.antsdb.saltedfish.sql.vdm.FuncLower;
import com.antsdb.saltedfish.sql.vdm.FuncMax;
import com.antsdb.saltedfish.sql.vdm.FuncMin;
import com.antsdb.saltedfish.sql.vdm.FuncMonth;
import com.antsdb.saltedfish.sql.vdm.FuncNow;
import com.antsdb.saltedfish.sql.vdm.FuncRand;
import com.antsdb.saltedfish.sql.vdm.FuncReleaseLock;
import com.antsdb.saltedfish.sql.vdm.FuncRound;
import com.antsdb.saltedfish.sql.vdm.FuncSubstringIndex;
import com.antsdb.saltedfish.sql.vdm.FuncSum;
import com.antsdb.saltedfish.sql.vdm.FuncToTimestamp;
import com.antsdb.saltedfish.sql.vdm.FuncTrim;
import com.antsdb.saltedfish.sql.vdm.FuncUnixTimestamp;
import com.antsdb.saltedfish.sql.vdm.FuncUpper;
import com.antsdb.saltedfish.sql.vdm.FuncUser;
import com.antsdb.saltedfish.sql.vdm.FuncVersion;
import com.antsdb.saltedfish.sql.vdm.FuncWeekday;
import com.antsdb.saltedfish.sql.vdm.FuncYear;
import com.antsdb.saltedfish.sql.vdm.Function;
import com.antsdb.saltedfish.sql.vdm.LongValue;
import com.antsdb.saltedfish.sql.vdm.NullValue;
import com.antsdb.saltedfish.sql.vdm.NumericValue;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.OpAdd;
import com.antsdb.saltedfish.sql.vdm.OpAddTime;
import com.antsdb.saltedfish.sql.vdm.OpAnd;
import com.antsdb.saltedfish.sql.vdm.OpBetween;
import com.antsdb.saltedfish.sql.vdm.OpBinary;
import com.antsdb.saltedfish.sql.vdm.OpBitwiseAnd;
import com.antsdb.saltedfish.sql.vdm.OpCase;
import com.antsdb.saltedfish.sql.vdm.OpConcat;
import com.antsdb.saltedfish.sql.vdm.OpDivide;
import com.antsdb.saltedfish.sql.vdm.OpEqual;
import com.antsdb.saltedfish.sql.vdm.OpEqualNull;
import com.antsdb.saltedfish.sql.vdm.OpExists;
import com.antsdb.saltedfish.sql.vdm.OpInSelect;
import com.antsdb.saltedfish.sql.vdm.OpInValues;
import com.antsdb.saltedfish.sql.vdm.OpInterval;
import com.antsdb.saltedfish.sql.vdm.OpIsNull;
import com.antsdb.saltedfish.sql.vdm.OpLarger;
import com.antsdb.saltedfish.sql.vdm.OpLargerEqual;
import com.antsdb.saltedfish.sql.vdm.OpLess;
import com.antsdb.saltedfish.sql.vdm.OpLessEqual;
import com.antsdb.saltedfish.sql.vdm.OpLike;
import com.antsdb.saltedfish.sql.vdm.OpMatch;
import com.antsdb.saltedfish.sql.vdm.OpMultiply;
import com.antsdb.saltedfish.sql.vdm.OpNegate;
import com.antsdb.saltedfish.sql.vdm.OpNot;
import com.antsdb.saltedfish.sql.vdm.OpOr;
import com.antsdb.saltedfish.sql.vdm.OpRegexp;
import com.antsdb.saltedfish.sql.vdm.OpSequenceValue;
import com.antsdb.saltedfish.sql.vdm.OpSingleValueQuery;
import com.antsdb.saltedfish.sql.vdm.Operator;
import com.antsdb.saltedfish.sql.vdm.RowidValue;
import com.antsdb.saltedfish.sql.vdm.SysDate;
import com.antsdb.saltedfish.sql.vdm.SystemVariableValue;
import com.antsdb.saltedfish.sql.vdm.BinaryString;
import com.antsdb.saltedfish.sql.vdm.UserVariableValue;
import com.antsdb.saltedfish.util.BytesUtil;
import com.antsdb.saltedfish.util.Pair;

public class ExprGenerator {
    static Map<String, Class<? extends Function>> _functionByName = new HashMap<>();

    static {
        _functionByName.put("abs", FuncAbs.class);
        _functionByName.put("adddate", FuncDateAdd.class);
        _functionByName.put("concat", FuncConcat.class);
        _functionByName.put("curdate", FuncCurDate.class);
        _functionByName.put("current_user", FuncCurrentUser.class);
        _functionByName.put("connection_id", FuncConnectionId.class);
        _functionByName.put("database", FuncDatabase.class);
        _functionByName.put("date_add", FuncDateAdd.class);
        _functionByName.put("date_format", FuncDateFormat.class);
        _functionByName.put("date_sub", FuncDateSub.class);
        _functionByName.put("elt", FuncElt.class);
        _functionByName.put("empty_clob", FuncEmptyClob.class);
        _functionByName.put("field", FuncField.class);
        _functionByName.put("find_in_set", FuncFindInSet.class);
        _functionByName.put("from_unixtime", FuncFromUnixTime.class);
        _functionByName.put("get_lock", FuncGetLock.class);
        _functionByName.put("hex", FuncHex.class);
        _functionByName.put("if", FuncIf.class);
        _functionByName.put("length", FuncLength.class);
        _functionByName.put("left", FuncLeft.class);
        _functionByName.put("locate", FuncLocate.class);
        _functionByName.put("lower", FuncLower.class);
        _functionByName.put("month", FuncMonth.class);
        _functionByName.put("now", FuncNow.class);
        _functionByName.put("rand", FuncRand.class);
        _functionByName.put("release_lock", FuncReleaseLock.class);
        _functionByName.put("round", FuncRound.class);
        _functionByName.put("subdate", FuncDateSub.class);
        _functionByName.put("substring_index", FuncSubstringIndex.class);
        _functionByName.put("totimestamp", FuncToTimestamp.class);
        _functionByName.put("to_base64", FuncBase64.class);
        _functionByName.put("to_timestamp", FuncToTimestamp.class);
        _functionByName.put("trim", FuncTrim.class);
        _functionByName.put("unix_timestamp", FuncUnixTimestamp.class);
        _functionByName.put("user", FuncUser.class);
        _functionByName.put("upper", FuncUpper.class);
        _functionByName.put("user", FuncUser.class);
        _functionByName.put("version", FuncVersion.class);
        _functionByName.put("weekday", FuncWeekday.class);
        _functionByName.put("year", FuncYear.class);
    }

    public static Operator gen(GeneratorContext ctx, Planner planner, ExprContext rule) {
        Operator op = gen_(ctx, planner, rule);
        return op;
    }

    static Operator gen(GeneratorContext ctx, Planner planner, Expr_simpleContext rule) {
        ParseTree child = rule.getChild(0);
        Operator result;
        if (child instanceof Literal_valueContext) {
            result = genLiteralValue(ctx, planner, (Literal_valueContext)child);
        }
        else if (child instanceof Bind_parameterContext) {
            result = genBindParameter(ctx, (Bind_parameterContext)child);
        }
        else if (child instanceof Column_name_Context) {
            result = genColumnValue(ctx, planner, (Column_name_Context)child);
        }
        else if (child instanceof Variable_referenceContext) {
            result = genUserVariableRef(ctx, planner, (Variable_referenceContext)child);
        }
        else if (child instanceof Session_variable_referenceContext) {
            result = genSystemVariableRef(ctx, planner, (Session_variable_referenceContext)child);
        }
        else if (child instanceof Expr_functionContext) {
            result = genFunction(ctx, planner, (Expr_functionContext)child);
        }
        else if (child instanceof Expr_selectContext) {
            result = genSingleValueQuery(ctx, planner, (Expr_selectContext)child);
        }
        else {
            throw new IllegalArgumentException();
        }
        return result;
    }
    
    public static Operator gen(GeneratorContext ctx, Planner planner, ValueContext rule) {
        ParseTree child = rule.getChild(0);
        if (child instanceof Literal_valueContext) {
            return genLiteralValue(ctx, planner, (Literal_valueContext) child);
        }
        else if (child instanceof Bind_parameterContext) {
            return genBindParameter(ctx, (Bind_parameterContext) child);
        }
        else if (child instanceof Column_name_Context) {
            return genColumnValue(ctx, planner, (Column_name_Context) child);
        }
        else if (child instanceof Variable_referenceContext) {
            return genUserVariableRef(ctx, planner, (Variable_referenceContext) child);
        }
        else if (child instanceof Session_variable_referenceContext) {
            return genSystemVariableRef(ctx, planner, (Session_variable_referenceContext) child);
        }
        else {
            throw new NotImplementedException();
        }
    }
    
    static Operator gen_(GeneratorContext ctx, Planner cursorMeta, ExprContext rule) throws OrcaException {
        if (rule.expr_not() != null) {
            return gen_not(ctx, cursorMeta, rule.expr_not());
        }
        else if (rule.value() != null) {
            return gen(ctx, cursorMeta, rule.value());
        }
        else if (rule.getChildCount() == 1) {
            return gen_sinlge_node(ctx, cursorMeta, rule);
        }
        else if (rule.expr_in_values() != null) {
            Operator left = gen(ctx, cursorMeta, (ExprContext) rule.getChild(0));
            return gen_in_values(ctx, cursorMeta, rule.expr_in_values(), left);
        }
        else if (rule.expr_in_select() != null) {
            Operator left = gen(ctx, cursorMeta, (ExprContext) rule.getChild(0));
            return gen_in_select(ctx, cursorMeta, rule.expr_in_select(), left);
        }
        else if (rule.unary_operator() != null) {
            Operator right = gen(ctx, cursorMeta, (ExprContext) rule.getChild(1));
            return gen_unary(ctx, cursorMeta, rule.unary_operator(), right);
        }
        else if (rule.K_LIKE() != null) {
            Like_exprContext like = rule.like_expr();
            Operator left = gen(ctx, cursorMeta, (ExprContext) rule.getChild(0));
            Operator right = genLikeExpr(ctx, cursorMeta, like);
            Operator result = new OpLike(left, right);
            if (rule.K_NOT() != null) {
                result = new OpNot(result);
            }
            return result;
        }
        else if (rule.K_REGEXP() != null) {
            // REGEXP operator
            Operator expr = gen(ctx, cursorMeta, (ExprContext) rule.getChild(0));
            Operator pattern = genLikeExpr(ctx, cursorMeta, rule.like_expr());
            int variableId = ctx.allocVariable();
            return new OpRegexp(expr, pattern, variableId);
        }
        else if (rule.K_BETWEEN() != null) {
            Operator expr = gen(ctx, cursorMeta, rule.expr_simple(0));
            Operator from = gen(ctx, cursorMeta, rule.expr_simple(1));
            Operator to = gen(ctx, cursorMeta, rule.expr_simple(2));
            return new OpBetween(expr, from, to);
        }
        else if (rule.getChild(1) instanceof TerminalNode) {
            String op = rule.getChild(1).getText();
            if ("=".equals(op)) {
                Operator left = gen(ctx, cursorMeta, (ExprContext) rule.getChild(0));
                Operator right = gen(ctx, cursorMeta, (ExprContext) rule.getChild(2));
                return new OpEqual(left, right);
            }
            else if ("==".equals(op)) {
                Operator left = gen(ctx, cursorMeta, (ExprContext) rule.getChild(0));
                Operator right = gen(ctx, cursorMeta, (ExprContext) rule.getChild(2));
                return new OpEqualNull(left, right);
            }
            else if ("+".equals(op)) {
                Operator left = gen(ctx, cursorMeta, (ExprContext) rule.getChild(0));
                Operator right = gen(ctx, cursorMeta, (ExprContext) rule.getChild(2));
                if ((left instanceof OpInterval) || (right instanceof OpInterval)) {
                    return new OpAddTime(left, right);
                }
                else {
                    return new OpAdd(left, right);
                }
            }
            else if ("-".equals(op)) {
                Operator left = gen(ctx, cursorMeta, (ExprContext) rule.getChild(0));
                Operator right = gen(ctx, cursorMeta, (ExprContext) rule.getChild(2));
                if ((left instanceof OpInterval) || (right instanceof OpInterval)) {
                    return new OpAddTime(left, new OpNegate(right));
                }
                else {
                    return new OpAdd(left, new OpNegate(right));
                }
            }
            else if ("*".equals(op)) {
                Operator left = gen(ctx, cursorMeta, (ExprContext) rule.getChild(0));
                Operator right = gen(ctx, cursorMeta, (ExprContext) rule.getChild(2));
                return new OpMultiply(left, right);
            }
            else if ("/".equals(op)) {
                Operator left = gen(ctx, cursorMeta, (ExprContext) rule.getChild(0));
                Operator right = gen(ctx, cursorMeta, (ExprContext) rule.getChild(2));
                return new OpDivide(left, right);
            }
            else if (">".equals(op)) {
                Operator left = gen(ctx, cursorMeta, (ExprContext) rule.getChild(0));
                Operator right = gen(ctx, cursorMeta, (ExprContext) rule.getChild(2));
                return new OpLarger(left, right);
            }
            else if (">=".equals(op)) {
                Operator left = gen(ctx, cursorMeta, (ExprContext) rule.getChild(0));
                Operator right = gen(ctx, cursorMeta, (ExprContext) rule.getChild(2));
                return new OpLargerEqual(left, right);
            }
            else if ("<".equals(op)) {
                Operator left = gen(ctx, cursorMeta, (ExprContext) rule.getChild(0));
                Operator right = gen(ctx, cursorMeta, (ExprContext) rule.getChild(2));
                return new OpLess(left, right);
            }
            else if ("<=".equals(op)) {
                Operator left = gen(ctx, cursorMeta, (ExprContext) rule.getChild(0));
                Operator right = gen(ctx, cursorMeta, (ExprContext) rule.getChild(2));
                return new OpLessEqual(left, right);
            }
            else if ("!=".equals(op)) {
                Operator left = gen(ctx, cursorMeta, (ExprContext) rule.getChild(0));
                Operator right = gen(ctx, cursorMeta, (ExprContext) rule.getChild(2));
                return new OpNot(new OpEqual(left, right));
            }
            else if ("<>".equals(op)) {
                Operator left = gen(ctx, cursorMeta, (ExprContext) rule.getChild(0));
                Operator right = gen(ctx, cursorMeta, (ExprContext) rule.getChild(2));
                return new OpNot(new OpEqual(left, right));
            }
            else if ("&".equals(op)) {
                Operator left = gen(ctx, cursorMeta, (ExprContext) rule.getChild(0));
                Operator right = gen(ctx, cursorMeta, (ExprContext) rule.getChild(2));
                return new OpBitwiseAnd(left, right);
            }
            else if ("or".equalsIgnoreCase(op)) {
                Operator left = gen(ctx, cursorMeta, (ExprContext) rule.getChild(0));
                Operator right = gen(ctx, cursorMeta, (ExprContext) rule.getChild(2));
                return new OpOr(left, right);
            }
            else if ("and".equalsIgnoreCase(op)) {
                Operator left = gen(ctx, cursorMeta, (ExprContext) rule.getChild(0));
                Operator right = gen(ctx, cursorMeta, (ExprContext) rule.getChild(2));
                return new OpAnd(left, right);
            }
            else if ("||".equals(op)) {
                Operator left = gen(ctx, cursorMeta, (ExprContext) rule.getChild(0));
                Operator right = gen(ctx, cursorMeta, (ExprContext) rule.getChild(2));
                return new OpConcat(left, right);
            }
            else if ("is".equalsIgnoreCase(op)) {
                if (rule.getChild(2) instanceof ExprContext) {
                    Operator upstream = gen(ctx, cursorMeta, (ExprContext) rule.getChild(0));
                    return new OpIsNull(upstream);
                }
                else if (rule.getChild(2) instanceof TerminalNode) {
                    // IS NOT
                    Operator upstream = gen(ctx, cursorMeta, (ExprContext) rule.getChild(0));
                    return new OpNot(new OpIsNull(upstream));
                }
                else {
                    throw new NotImplementedException();
                }
            }
            else if ("not".equalsIgnoreCase(op)) {
                if (rule.getChild(2) instanceof TerminalNode) {
                    TerminalNode node = (TerminalNode) rule.getChild(2);
                    if (node.getSymbol().getType() == MysqlParser.K_LIKE) {
                        Operator left = gen(ctx, cursorMeta, (ExprContext) rule.getChild(0));
                        Operator right = genLikeExpr(ctx, cursorMeta, rule.like_expr());
                        return new OpNot(new OpLike(left, right));
                    }
                }
                throw new NotImplementedException();
            }
            else {
                throw new NotImplementedException();
            }
        }
        else if (rule.K_DISTINCT() != null) {
            Operator op = gen(ctx, cursorMeta, rule.expr(0));
            return new FuncDistinct(op, ctx.allocVariable());
        }
        else {
            String text = rule.getText();
            throw new NotImplementedException(text);
        }
    }

    private static Operator gen_not(GeneratorContext ctx, Planner planner, Expr_notContext rule) {
        Operator expr = gen(ctx, planner, rule.expr());
        return new OpNot(expr);
    }

    private static Operator genLikeExpr(GeneratorContext ctx, Planner cursorMeta, Like_exprContext rule) {
        return gen(ctx, cursorMeta, rule.expr_simple());
    }

    private static Operator gen_unary(
            GeneratorContext ctx, 
            Planner cursorMeta, 
            Unary_operatorContext rule,
            Operator right) {
        TerminalNode token = (TerminalNode) rule.getChild(0);
        if (token.getSymbol().getType() == MysqlParser.MINUS) {
            return new OpNegate(right);
        }
        else if (token.getSymbol().getType() == MysqlParser.K_BINARY) {
            return new OpBinary(right);
        }
        else {
            throw new NotImplementedException();
        }
    }

    private static Operator gen_exists(GeneratorContext ctx, Planner cursorMeta, Expr_existContext rule) {
        if (rule.select_stmt().select_or_values().size() != 1) {
            throw new NotImplementedException();
        }
        CursorMaker select = (CursorMaker) new Select_or_valuesGenerator().genSubquery(
                ctx, 
                rule.select_stmt(),
                cursorMeta);
        Operator in = new OpExists(select);
        if (rule.K_NOT() != null) {
            in = new OpNot(in);
        }
        return in;
    }

    private static Operator gen_in_select(
            GeneratorContext ctx, 
            Planner cursorMeta, 
            Expr_in_selectContext rule,
            Operator left) {
        if (rule.select_stmt().select_or_values().size() != 1) {
            throw new NotImplementedException();
        }
        /*
         * workaround for topka query CursorMaker select = (CursorMaker)new
         * Select_or_valuesGenerator().genSubquery( ctx, rule.select_stmt(),
         * cursorMeta);
         */
        CursorMaker select = (CursorMaker) new Select_stmtGenerator().gen(ctx, rule.select_stmt());
        Operator in = new OpInSelect(left, select);
        if (rule.K_NOT() != null) {
            in = new OpNot(in);
        }
        return in;
    }

    private static Operator gen_in_values(GeneratorContext ctx, Planner cursorMeta, Expr_in_valuesContext rule,
            Operator left) {
        List<Operator> values = new ArrayList<Operator>();
        for (ExprContext i : rule.expr()) {
            Operator expr = gen(ctx, cursorMeta, i);
            values.add(expr);
        }
        return new OpInValues(left, values);
    }

    private static Operator genSingleValueQuery(GeneratorContext ctx, Planner cursorMeta, Expr_selectContext rule) {
        if (rule.select_stmt().select_or_values().size() != 1) {
            throw new NotImplementedException();
        }
        CursorMaker select = (CursorMaker) new Select_or_valuesGenerator().genSubquery(ctx, rule.select_stmt(),
                cursorMeta);
        if (select.getCursorMeta().getColumnCount() != 1) {
            throw new OrcaException("Operand should contain 1 column");
        }
        return new OpSingleValueQuery(select);
    }

    public static Operator gen_sinlge_node(GeneratorContext ctx, Planner cursorMeta, ExprContext rule) {
        ParseTree child = rule.getChild(0);
        if (child instanceof Expr_functionContext) {
            return genFunction(ctx, cursorMeta, (Expr_functionContext) child);
        }
        else if (child instanceof Expr_parenthesisContext) {
            return gen(ctx, cursorMeta, ((Expr_parenthesisContext) child).expr());
        }
        else if (child instanceof Expr_selectContext) {
            return genSingleValueQuery(ctx, cursorMeta, (Expr_selectContext) child);
        }
        else if (child instanceof Column_name_Context) {
            return genColumnValue(ctx, cursorMeta, (Column_name_Context) child);
        }        
        else if (rule.expr_exist() != null) {
            return gen_exists(ctx, cursorMeta, rule.expr_exist());
        }
        else if (rule.expr_cast() != null) {
            return gen_cast(ctx, cursorMeta, rule.expr_cast());
        }
        else if (rule.expr_match() != null) {
            return gen_match(ctx, cursorMeta, rule.expr_match());
        }
        else if (rule.expr_case() != null) {
            return gen_case(ctx, cursorMeta, rule.expr_case());
        }
        else {
            throw new NotImplementedException();
        }
    }

    private static Operator gen_match(GeneratorContext ctx, Planner planner, Expr_matchContext rule) {
        boolean isBooleanMode = rule.K_BOOLEAN() != null;
        List<FieldValue> columns = new ArrayList<>();
        rule.column_name_().forEach((it) -> {
            Operator op = genColumnValue(ctx, planner, it);
            if (!(op instanceof FieldValue)) {
                throw new OrcaException("{} is not an column reference", op);
            }
            columns.add((FieldValue) op);
        });
        if (columns.size() <= 0) {
            throw new OrcaException("column reference is missing");
        }
        Operator against = gen(ctx, planner, rule.value());
        return new OpMatch(columns, against, isBooleanMode);
    }

    private static Operator gen_cast(GeneratorContext ctx, Planner cursorMeta, Expr_castContext rule) {
        DataType type = DataType.parse(ctx.getTypeFactory(), rule.data_type());
        Operator expr = gen(ctx, cursorMeta, rule.expr());
        return new FuncCast(type, expr);
    }

    static Operator genSystemVariableRef(GeneratorContext ctx, Planner meta, Session_variable_referenceContext rule) {
        String name = rule.getText().substring(2);
        return new SystemVariableValue(name);
    }

    static Operator genUserVariableRef(GeneratorContext ctx, Planner meta, Variable_referenceContext rule) {
        String name = rule.getText().substring(1);
        return new UserVariableValue(name);
    }

    private static Operator genFunction(GeneratorContext ctx, Planner cursorMeta, Expr_functionContext rule) {
        String name = rule.function_name().getText().toLowerCase();
        Function func = null;
        try {
            Class<? extends Function> klass = _functionByName.get(name);
            if (klass != null) {
                func = _functionByName.get(name).newInstance();
            }
        }
        catch (Exception x) {
            throw new OrcaException(x);
        }
        if (func == null) {
            // aggregate functions below
            if (name.equalsIgnoreCase("count")) {
                if (rule.expr_function_star_parameter() != null) {
                    func = new FuncCount(ctx.allocVariable());
                    func.addParameter(null);
                }
                else {
                    if (rule.expr_function_parameters().expr().size() != 1) {
                        throw new OrcaException("count takes one and only one input parameter");
                    }
                    func = new FuncCount(ctx.allocVariable());
                }
            }
            else if (name.equalsIgnoreCase("max")) {
                func = new FuncMax(ctx.allocVariable());
            }
            else if (name.equalsIgnoreCase("min")) {
                func = new FuncMin(ctx.allocVariable());
            }
            else if (name.equalsIgnoreCase("sum")) {
                if (rule.expr_function_parameters() == null) {
                    throw new OrcaException("count takes one and only one input parameter");
                }
                if (rule.expr_function_parameters().expr().size() != 1) {
                    throw new OrcaException("count takes one and only one input parameter");
                }
                Operator expr = ExprGenerator.gen(ctx, cursorMeta, rule.expr_function_parameters().expr(0));
                func = new FuncSum(ctx.allocVariable(), expr);
            }
            else if (name.equalsIgnoreCase("avg")) {
                if (rule.expr_function_parameters() == null) {
                    throw new OrcaException("count takes one and only one input parameter");
                }
                if (rule.expr_function_parameters().expr().size() != 1) {
                    throw new OrcaException("count takes one and only one input parameter");
                }
                Operator expr = ExprGenerator.gen(ctx, cursorMeta, rule.expr_function_parameters().expr(0));
                func = new FuncAvg(ctx.allocVariable(), expr);
            }
            else if (name.equalsIgnoreCase("GROUP_CONCAT")) {
                func = genGroupConcat(ctx, rule);
            }
            else {
                throw new NotImplementedException("function: " + name);
            }
        }
        if (rule.expr_function_parameters() != null) {
            for (ExprContext i : rule.expr_function_parameters().expr()) {
                func.addParameter(ExprGenerator.gen(ctx, cursorMeta, i));
            }
        }
        if (rule.group_concat_parameter() != null) {
            Group_concat_parameterContext params = rule.group_concat_parameter();
            Operator col = genColumnValue(ctx, cursorMeta, params.column_name_());
            func.addParameter(col);
            if (params.literal_value() != null) {
                Operator separator = genLiteralValue(ctx, cursorMeta, params.literal_value());
                func.addParameter(separator);
            }
        }
        if ((func.getMinParameters() >= 0) && (func.getChildren().size() < func.getMinParameters())) {
            throw new OrcaException("{}() requires at least {} parameters", name, func.getMinParameters());
        }
        if ((func.getMaxParameters() >= 0) && (func.getChildren().size() > func.getMaxParameters())) {
            throw new OrcaException("{}() cannot take more than {} parameters", name, func.getMaxParameters());
        }
        return func;
    }

    private static Function genGroupConcat(GeneratorContext ctx, Expr_functionContext rule) {
        if (rule.expr_function_parameters() != null) {
            if (rule.expr_function_parameters().expr().size() != 1) {
                throw new OrcaException("GROUP_CONCAT requires one input parameter");
            }
        }
        Group_concat_parameterContext param = rule.group_concat_parameter();
        Boolean asc = null;
        boolean distinct = false;
        if (param != null) {
            if (param.ordering_term() != null) {
                if (param.ordering_term().size() > 0) {
                    asc = param.ordering_term().get(0).K_DESC() == null;
                }
            }
            distinct = param.K_DISTINCT() != null;
        }
        FuncGroupConcat func = new FuncGroupConcat(ctx.allocVariable(), distinct, asc);
        return func;
    }

    private static Operator genColumnValue(GeneratorContext ctx, Planner planner, Column_name_Context rule) {
        if (planner == null) {
            // why there is no cursor metadata
            throw new OrcaException("missing query");
        }
        ObjectName tableName = TableName.parse(ctx, rule.identifier());
        String table = (tableName != null) ? tableName.getTableName() : null;
        String column = Utils.getIdentifier(rule.column_name().identifier());

        // check built-in name

        if ("rowid".equalsIgnoreCase(column)) {
            return new RowidValue();
        }
        if ("nextval".equalsIgnoreCase(column)) {
            return genSequenceValue(ctx, rule);
        }

        // check table columns

        PlannerField pos = planner.findField(it -> {
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
        if (pos == null) {
            throw new OrcaException("column not found: " + column);
        }
        FieldValue cv = new FieldValue(pos);
        return cv;
    }

    private static Operator genSequenceValue(GeneratorContext ctx, Column_name_Context rule) {
        ObjectName name = TableName.parse(ctx, rule.identifier());
        return new OpSequenceValue(name);
    }

    public static Operator genLiteralValue(GeneratorContext ctx, Planner planner, Literal_valueContext rule) {
        if (rule.literal_value_binary() != null) {
            return new BinaryString(getBytes(rule.literal_value_binary()), true);
        }
        else if (rule.literal_interval() != null) {
            return parseInterval(ctx, planner, rule.literal_interval());
        }
        TerminalNode token = (TerminalNode) rule.getChild(0);
        switch (token.getSymbol().getType()) {
        case MysqlParser.NUMERIC_LITERAL: {
            BigDecimal bd = new BigDecimal(rule.getText());
            try {
                return new LongValue(bd.longValueExact());
            }
            catch (Exception ignored) {
            }
            return new NumericValue(new BigDecimal(rule.getText()));
        }
        case MysqlParser.STRING_LITERAL:
        case MysqlParser.DOUBLE_QUOTED_LITERAL:
            /* mysql strings are all binary */
            return new BinaryString(getBytes(token), false);
        case MysqlParser.K_NULL:
            return new NullValue();
        case MysqlParser.K_CURRENT_DATE:
            return new SysDate();
        case MysqlParser.K_CURRENT_TIME:
            return new CurrentTime();
        case MysqlParser.K_CURRENT_TIMESTAMP:
            return new CurrentTimestamp();
        case MysqlParser.K_TRUE:
            return new LongValue(1);
        case MysqlParser.K_FALSE:
            return new LongValue(0);
        case MysqlParser.BLOB_LITERAL:
            String text = rule.BLOB_LITERAL().getText();
            return new BytesValue(mysqlXBinaryToBytes(text));
        case MysqlParser.HEX_LITERAL:
            String hextxt = rule.HEX_LITERAL().getText();
            hextxt = hextxt.substring(2, hextxt.length());
            if (hextxt.length() == 0) {
                return new BytesValue(new byte[0]);
            }
            return new BytesValue(BytesUtil.hexToBytes(hextxt));
        default:
            throw new NotImplementedException();
        }
    }

    private static Operator parseInterval(GeneratorContext ctx, Planner planner, Literal_intervalContext rule) {
        Operator upstream = gen(ctx, planner, rule.expr());
        String unit = rule.WORD().getText();
        long multiplier;
        if (unit.equalsIgnoreCase("SECOND")) {
            multiplier = 1000;
        }
        else if (unit.equalsIgnoreCase("MINUTE")) {
            multiplier = 1000 * 60;
        }
        else if (unit.equalsIgnoreCase("HOUR")) {
            multiplier = 1000 * 60 * 60;
        }
        else if (unit.equalsIgnoreCase("DAY")) {
            multiplier = 1000 * 60 * 60 * 24;
        }
        else if (unit.equalsIgnoreCase("WEEK")) {
            multiplier = 1000 * 60 * 60 * 24 * 7;
        }
        else {
            throw new OrcaException("unknown unit of time: " + unit);
        }
        return new OpInterval(upstream, multiplier);
    }

    private static byte[] mysqlXBinaryToBytes(String text) {
        if (text.length() < 3) {
            throw new IllegalArgumentException("invalid binary format: " + text);
        }
        if (text.length() % 2 != 1) {
            throw new IllegalArgumentException("invalid binary format: " + text);
        }
        byte[] bytes = new byte[(text.length() - 3) / 2];
        for (int i = 2; i < text.length() - 1; i += 2) {
            int ch1 = text.charAt(i);
            int ch2 = text.charAt(i + 1);
            ch1 = Character.digit(ch1, 16);
            ch2 = Character.digit(ch2, 16);
            int n = ch1 << 4 | ch2;
            bytes[i / 2 - 1] = (byte) n;
        }
        return bytes;
    }

    private static byte[] getBytes(Literal_value_binaryContext rule) {
        return getBytes(rule.STRING_LITERAL());
    }
    
    private static byte[] getBytes(TerminalNode rule) {
        Token token = rule.getSymbol();
        byte[] bytes = new byte[token.getStopIndex() - token.getStartIndex() - 1];
        CharStream cs = token.getInputStream();
        int pos = cs.index();
        cs.seek(token.getStartIndex() + 1);
        int j = 0;
        for (int i = 0; i < bytes.length; i++) {
            int ch = cs.LA(i + 1);
            if (ch == '\\') {
                i++;
                ch = cs.LA(i + 1);
                if (ch == '0') {
                    ch = 0;
                }
                else if (ch == 'n') {
                    ch = '\n';
                }
                else if (ch == 'r') {
                    ch = '\r';
                }
                else if (ch == 'Z') {
                    ch = '\032';
                }
            }
            bytes[j] = (byte) ch;
            j++;
        }
        cs.seek(pos);
        if (j != bytes.length) {
            // esacpe characters
            byte[] old = bytes;
            bytes = new byte[j];
            System.arraycopy(old, 0, bytes, 0, j);
        }
        return bytes;
    }

    private static Operator genBindParameter(GeneratorContext ctx, Bind_parameterContext rule) {
        return new BindParameter(ctx.getParameterPosition(rule.BIND_PARAMETER()));
    }

    public static Operator gen(GeneratorContext ctx, Planner cursorMeta, String expr) {
        CharStream cs = new ANTLRInputStream(expr);
        MysqlLexer lexer = new MysqlLexer(cs);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.setTokenSource(lexer);
        MysqlParser parser = new MysqlParser(tokens);
        parser.setErrorHandler(new BailErrorStrategy());
        MysqlParser.ExprContext rule = parser.expr();
        return gen(ctx, cursorMeta, rule);
    }
    
    private static Operator gen_case(GeneratorContext ctx, Planner planner, Expr_caseContext rule) {
        List<Pair<Operator,Operator>> whens = new ArrayList<>();
        Operator alse = null;
        Operator value = gen(ctx, planner, rule.expr());
        for (Expr_case_whenContext i:rule.expr_case_when()) {
            Pair<Operator,Operator> pair = new Pair<>();
            pair.x = gen(ctx, planner, i.expr(0));
            pair.y = gen(ctx, planner, i.expr(1));
            whens.add(pair);
        }
        if (rule.expr_case_else() != null) {
            alse = gen(ctx, planner, rule.expr_case_else().expr());
        }
        return new OpCase(value, whens, alse);
    }

}
