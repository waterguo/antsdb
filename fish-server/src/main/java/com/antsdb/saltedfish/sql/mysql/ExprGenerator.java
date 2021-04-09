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
import java.util.function.IntSupplier;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.charset.Codecs;
import com.antsdb.saltedfish.charset.Decoder;
import com.antsdb.saltedfish.lexer.MysqlLexer;
import com.antsdb.saltedfish.lexer.MysqlParser;
import com.antsdb.saltedfish.lexer.MysqlParser.Bind_parameterContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Column_name_Context;
import com.antsdb.saltedfish.lexer.MysqlParser.ExprContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_additiveContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_betweenContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_caseContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_case_whenContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_castContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_cast_data_typeContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_compareContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_existContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_functionContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_in_selectContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_in_valuesContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_isContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_matchContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_multiContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_notContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_numericContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_parenthesisContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_primaryContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_relationalContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_searchContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_selectContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_simpleContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Expr_unaryContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Group_concat_parameterContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Like_exprContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Literal_intervalContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Literal_stringContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Literal_valueContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Session_variable_referenceContext;
import com.antsdb.saltedfish.lexer.MysqlParser.ValueContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Variable_referenceContext;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.DataTypeFactory;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.planner.Planner;
import com.antsdb.saltedfish.sql.planner.PlannerField;
import com.antsdb.saltedfish.sql.vdm.*;
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
        _functionByName.put("dayofmonth", FuncDayOfMonth.class);
        _functionByName.put("dayofweek", FuncDayOfWeek.class);
        _functionByName.put("dayofyear", FuncDayOfYear.class);
        _functionByName.put("elt", FuncElt.class);
        _functionByName.put("empty_clob", FuncEmptyClob.class);
        _functionByName.put("field", FuncField.class);
        _functionByName.put("find_in_set", FuncFindInSet.class);
        _functionByName.put("from_unixtime", FuncFromUnixTime.class);
        _functionByName.put("get_lock", FuncGetLock.class);
        _functionByName.put("hex", FuncHex.class);
        _functionByName.put("if", FuncIf.class);
        _functionByName.put("ifnull", FuncIfNull.class);
        _functionByName.put("isnull", FuncIsNull.class);
        _functionByName.put("last_insert_id", FuncLastInsertId.class);
        _functionByName.put("length", FuncLength.class);
        _functionByName.put("left", FuncLeft.class);
        _functionByName.put("locate", FuncLocate.class);
        _functionByName.put("lower", FuncLower.class);
        _functionByName.put("mod", FuncMod.class);
        _functionByName.put("month", FuncMonth.class);
        _functionByName.put("monthname", FuncMonthName.class);
        _functionByName.put("now", FuncNow.class);
        _functionByName.put("rand", FuncRand.class);
        _functionByName.put("release_lock", FuncReleaseLock.class);
        _functionByName.put("replace", FuncReplace.class);
        _functionByName.put("round", FuncRound.class);
        _functionByName.put("subdate", FuncDateSub.class);
        _functionByName.put("substr", FuncSubstring.class);
        _functionByName.put("substring", FuncSubstring.class);
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
        else if (child instanceof Expr_simpleContext) {
            String op = rule.getChild(1).getText();
            if ("+".equals(op)) {
                Operator left = gen(ctx, planner, rule.expr_simple(0));
                Operator right = gen(ctx, planner, rule.expr_simple(1));
                if ((left instanceof OpInterval) || (right instanceof OpInterval)) {
                    result = new OpAddTime(left, right);
                }
                else {
                    result = new OpAdd(left, right);
                }
            }
            else if ("-".equals(op)) {
                Operator left = gen(ctx, planner, rule.expr_simple(0));
                Operator right = gen(ctx, planner, rule.expr_simple(1));
                if ((left instanceof OpInterval) || (right instanceof OpInterval)) {
                    result = new OpAddTime(left, new OpNegate(right));
                }
                else {
                    result = new OpAdd(left, new OpNegate(right));
                }
            }
            else if ("*".equals(op)) {
                Operator left = gen(ctx, planner, rule.expr_simple(0));
                Operator right = gen(ctx, planner, rule.expr_simple(1));
                result = new OpMultiply(left, right);
            }
            else if ("/".equals(op)) {
                Operator left = gen(ctx, planner, rule.expr_simple(0));
                Operator right = gen(ctx, planner, rule.expr_simple(1));
                result = new OpDivide(left, right);
            }
            else if ("%".equals(op) || "MOD".equalsIgnoreCase(op)) {
                Operator left = gen(ctx, planner, rule.expr_simple(0));
                Operator right = gen(ctx, planner, rule.expr_simple(1));
                FuncMod mod = new FuncMod();
                mod.addParameter(left);
                mod.addParameter(right);
                result = mod;
            }
            else if ("||".equals(op)) {
                Operator left = gen(ctx, planner, rule.expr_simple(0));
                Operator right = gen(ctx, planner, rule.expr_simple(1));
                return new OpConcat(left, right);
            }
            else {
                throw new IllegalArgumentException(op);
            }
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
        if (rule.value() != null) {
            return gen(ctx, cursorMeta, rule.value());
        }
        else if (rule.expr_unary() != null) {
            return gen_unary(ctx, cursorMeta, rule.expr_unary());
        }
        else if (rule.expr_not() != null) {
            return gen(ctx, cursorMeta, rule.expr_not());
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
            else if ("<=>".equals(op)) {
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
            else if ("%".equals(op)) {
                Operator left = gen(ctx, cursorMeta, (ExprContext) rule.getChild(0));
                Operator right = gen(ctx, cursorMeta, (ExprContext) rule.getChild(2));
                FuncMod result = new FuncMod();
                result.addParameter(left);
                result.addParameter(right);
                return result;
            }
            else if ("MOD".equalsIgnoreCase(op)) {
                Operator left = gen(ctx, cursorMeta, (ExprContext) rule.getChild(0));
                Operator right = gen(ctx, cursorMeta, (ExprContext) rule.getChild(2));
                FuncMod result = new FuncMod();
                result.addParameter(left);
                result.addParameter(right);
                return result;
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

    private static Operator gen(GeneratorContext ctx, Planner planner, Expr_notContext rule) {
        Operator right = gen(ctx, planner, rule.expr_relational());
        return new OpNot(right);
    }

    private static Operator gen(GeneratorContext ctx, Planner planner, Expr_relationalContext rule) {
        if (rule.expr_compare() != null) {
            return gen(ctx, planner, rule.expr_compare());
        }
        if (rule.expr_in_select() != null) {
            Operator left = gen(ctx, planner, rule.expr_numeric());
            return gen_in_select(ctx, planner, rule.expr_in_select(), left);
        }
        if (rule.expr_in_values() != null) {
            Operator left = gen(ctx, planner, rule.expr_numeric());
            return gen_in_values(ctx, planner, rule.expr_in_values(), left);
        }
        if (rule.expr_is() != null) {
            return gen(ctx, planner, rule.expr_is());
        }
        if (rule.expr_match() != null) {
            return gen(ctx, planner, rule.expr_match());
        }
        if (rule.expr_between() != null) {
            return gen(ctx, planner, rule.expr_between());
        }
        return gen(ctx, planner, rule.expr_numeric());
    }

    private static Operator gen(GeneratorContext ctx, Planner planner, Expr_betweenContext rule) {
        Operator expr = gen(ctx, planner, rule.expr_numeric(0));
        Operator from = gen(ctx, planner, rule.expr_numeric(1));
        Operator to = gen(ctx, planner, rule.expr_numeric(2));
        return new OpBetween(expr, from, to);
    }

    private static Operator gen(GeneratorContext ctx, Planner planner, Expr_matchContext rule) {
        Operator result;
        if (rule.K_LIKE() != null) {
            Operator left = gen(ctx, planner, rule.expr_numeric(0));
            Operator right = genLikeExpr(ctx, planner, rule.like_expr());
            result = new OpLike(left, right);
        }
        else if (rule.K_REGEXP() != null) {
            Operator expr = gen(ctx, planner, (ExprContext) rule.getChild(0));
            Operator pattern = genLikeExpr(ctx, planner, rule.like_expr());
            int variableId = ctx.allocVariable();
            result = new OpRegexp(expr, pattern, variableId);
        }
        else {
            throw new IllegalArgumentException();
        }
        return (rule.K_NOT() != null) ? new OpNot(result) : result;
    }

    private static Operator gen(GeneratorContext ctx, Planner planner, Expr_isContext rule) {
        Operator result = new OpIsNull(gen(ctx, planner, rule.expr_numeric()));
        return (rule.K_NOT() != null) ? new OpNot(result) : result;
    }

    private static Operator gen(GeneratorContext ctx, Planner planner, Expr_compareContext rule) {
        String op = rule.getChild(1).getText();
        Operator x = gen(ctx, planner, rule.expr_numeric(0));
        Operator y = gen(ctx, planner, rule.expr_numeric(1));
        if (">".equals(op)) {
            return new OpLarger(x, y);
        }
        else if (">=".equals(op)) {
            return new OpLargerEqual(x, y);
        }
        else if ("<".equals(op)) {
            return new OpLess(x, y);
        }
        else if ("<=".equals(op)) {
            return new OpLessEqual(x, y);
        }
        else if ("<>".equals(op)) {
            return new OpNot(new OpEqual(x, y));
        }
        else if ("=".equals(op)) {
            return new OpEqual(x, y);
        }
        else if ("==".equals(op)) {
            return new OpEqualNull(x, y);
        }
        else if ("<=>".equals(op)) {
            return new OpSafeEqual(x, y);
        }
        throw new IllegalArgumentException();
    }

    private static Operator gen(GeneratorContext ctx, Planner planner, Expr_numericContext rule) {
        return gen(ctx, planner, rule.expr_additive());
    }

    private static Operator gen(GeneratorContext ctx, Planner planner, Expr_additiveContext rule) {
        Operator result = gen(ctx, planner, rule.expr_multi(0));
        for (int i=1; i<rule.getChildCount(); i+=2) {
            String op = rule.getChild(i).getText();
            Operator x = gen(ctx, planner, (Expr_multiContext)rule.getChild(i+1));
            if (op.equals("+")) {
                result = new OpAdd(result, x);
            }
            else if (op.equals("-")) {
                result = new OpAdd(result, new OpNegate(x));
            }
            else if (op.equals("|")) {
                result = new OpBitwiseOr(result, x);
            }
            else {
                throw new IllegalArgumentException();
            }
        }
        return result;
    }

    private static Operator gen(GeneratorContext ctx, Planner planner, Expr_multiContext rule) {
        Operator result = gen_unary(ctx, planner, rule.expr_unary(0));
        for (int i=1; i<rule.getChildCount(); i+=2) {
            String op = rule.getChild(i).getText();
            Operator x = gen_unary(ctx, planner, (Expr_unaryContext)rule.getChild(i+1));
            if (op.equals("*")) {
                result = new OpMultiply(result, x);
            }
            else if (op.equals("/")) {
                result = new OpDivide(result, x);
            }
            else if (op.equals("&")) {
                result = new OpBitwiseAnd(result, x);
            }
            else if (op.equals("%") || op.equalsIgnoreCase("mod")) {
                FuncMod mod = new FuncMod();
                mod.addParameter(result);
                mod.addParameter(x);
                result = mod;
            }
            else {
                throw new IllegalArgumentException();
            }
        }
        return result;
    }

    private static Operator genLikeExpr(GeneratorContext ctx, Planner cursorMeta, Like_exprContext rule) {
        return gen(ctx, cursorMeta, rule.expr_simple());
    }

    private static Operator gen_unary(GeneratorContext ctx, Planner planner, Expr_unaryContext rule) {
        Operator result = gen_primary(ctx, planner, rule.expr_primary());
        if (rule.getChildCount() == 1) {
            return result;
        }
        else {
            TerminalNode token = (TerminalNode) rule.getChild(0);
            if (token.getSymbol().getType() == MysqlParser.MINUS) {
                return new OpNegate(result);
            }
            else if (token.getSymbol().getType() == MysqlParser.K_BINARY) {
                return new OpBinary(result);
            }
            else if (token.getSymbol().getType() == MysqlParser.K_NOT) {
                return new OpNot(result);
            }
            else if (token.getSymbol().getType() == MysqlParser.EXCLAIMATION) {
                return new OpNot(result);
            }
            else {
                throw new NotImplementedException();
            }
        }
    }

    private static Operator gen_primary(GeneratorContext ctx, Planner planner, Expr_primaryContext rule) {
        Operator result;
        if (rule.value() != null) {
            result = gen(ctx, planner, rule.value());
        }
        else if (rule.expr_function() != null) {
            result = genFunction(ctx, planner, rule.expr_function());
        }
        else if (rule.expr_parenthesis() != null) {
            result = gen(ctx, planner, rule.expr_parenthesis().expr());
        }
        else {
            throw new IllegalArgumentException("unknown rule: " + rule.getText());
        }
        return result;
    }

    private static Operator gen_exists(GeneratorContext ctx, Planner cursorMeta, Expr_existContext rule) {
        if (rule.select_stmt().select_or_values().size() != 1) {
            throw new NotImplementedException();
        }
        Planner planner = Select_or_valuesGenerator.genSubquery(ctx, rule.select_stmt(), cursorMeta);
        CursorMaker select = planner.run();
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
        Operator result = new OpInValues(left, values);
        if (rule.K_NOT() != null) {
            result = new OpNot(result);
        }        
        return result;
    }

    private static Operator genSingleValueQuery(GeneratorContext ctx, Planner cursorMeta, Expr_selectContext rule) {
        if (rule.select_stmt().select_or_values().size() != 1) {
            throw new NotImplementedException();
        }
        Planner planner = Select_or_valuesGenerator.genSubquery(ctx, rule.select_stmt(), cursorMeta);
        CursorMaker select = planner.run(cursorMeta.getWidth());
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
        else if (rule.expr_search() != null) {
            return gen_match(ctx, cursorMeta, rule.expr_search());
        }
        else if (rule.expr_case() != null) {
            return gen_case(ctx, cursorMeta, rule.expr_case());
        }
        else {
            throw new NotImplementedException();
        }
    }

    private static Operator gen_match(GeneratorContext ctx, Planner planner, Expr_searchContext rule) {
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
        DataType type = genCastType(ctx.getTypeFactory(), rule.expr_cast_data_type());
        Operator expr = gen(ctx, cursorMeta, rule.expr());
        return new FuncCast(type, expr);
    }

    private static DataType genCastType(DataTypeFactory fac, Expr_cast_data_typeContext rule) {
        String name = rule.any_name() != null ? rule.any_name().getText() : null;
        int length = 0;
        int scale = 0;
        if (name == null) {
            if (rule.K_SIGNED() != null) {
                name = "bigint";
            }
            else if (rule.K_UNSIGNED() != null) {
                name = "bigint";
            }
        }
        if (name == null) {
            throw new OrcaException("data type is not specified");
        }
        return fac.newDataType(name, length, scale);
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
                func = new FuncMax(ctx);
            }
            else if (name.equalsIgnoreCase("min")) {
                func = new FuncMin(ctx);
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
                Operator sum = new FuncSum(ctx.allocVariable(), expr);
                FuncCount count = new FuncCount(ctx.allocVariable());
                count.addParameter(expr);
                return new OpDivide(sum, count);
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
            FuncGroupConcat groupcat = (FuncGroupConcat)func;
            Group_concat_parameterContext params = rule.group_concat_parameter();
            Operator col = gen(ctx, cursorMeta, params.expr());
            groupcat.addParameter(col);
            if (params.literal_value() != null) {
                Operator separator = genLiteralValue(ctx, cursorMeta, params.literal_value());
                groupcat.setSeparator(separator);
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
            if (rule.expr_function_parameters().expr().size() < 1) {
                throw new OrcaException("GROUP_CONCAT requires at least one input parameter");
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
        if (rule.literal_string() != null) {
            return genString(ctx, planner, rule.literal_string());
        }
        else if (rule.literal_interval() != null) {
            return parseInterval(ctx, planner, rule.literal_interval());
        }
        else if (rule.current_timestamp_value() != null) {
            return new CurrentTimestamp();
        }
        else if (rule.current_time_value() != null) {
            return new CurrentTime();
        }
        else if (rule.signed_number() != null) {
            BigDecimal bd = new BigDecimal(rule.getText());
            try {
                return new LongValue(bd.longValueExact());
            }
            catch (Exception ignored) {
            }
            return new NumericValue(new BigDecimal(rule.getText()));
        }
        TerminalNode token = (TerminalNode) rule.getChild(0);
        switch (token.getSymbol().getType()) {
        case MysqlParser.K_NULL:
            return new NullValue();
        case MysqlParser.K_CURRENT_DATE:
            return new SysDate();
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

    private static Operator genString(GeneratorContext ctx, Planner planner, Literal_stringContext rule) {
        TerminalNode csctx = rule.CHARSET_NAME();
        Decoder decoder = null;
        if (csctx != null) {
            String csname = csctx.getText().substring(1);
            if (csname.equals("binary")) {
                return new BytesValue(getBytes(getStringNode(rule)));
            }
            decoder = Codecs.get(csname);
            if (decoder == null) {
                throw new OrcaException("{} is not a valid character set", csname); 
            }
        }
        if (decoder == null) {
            decoder = ctx.getSession().getConfig().getRequestDecoder();
        }
        return new BinaryString(getBytes(getStringNode(rule)), decoder);
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
        else if (unit.equalsIgnoreCase("MINUTE_SECOND")) {
            multiplier = 1000;
            return new OpInterval(new FuncParseMinuteSecond(upstream), multiplier);
        }
        else if (unit.equalsIgnoreCase("DAY_SECOND")) {
            multiplier = 1000;
            return new OpInterval(new FuncParseDaySecond(upstream), multiplier);
        }
        else if (unit.equalsIgnoreCase("DAY_HOUR")) {
            multiplier = 1000;
            return new OpInterval(new FuncParseDayHour(upstream), multiplier);
        }
        else if (unit.equalsIgnoreCase("SECOND_MICROSECOND")) {
            multiplier = 1;
            return new OpInterval(new FuncParseSecondMicro(upstream), multiplier);
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

    private static TerminalNode getStringNode(Literal_stringContext rule) {
        TerminalNode singleQuoted = rule.STRING_LITERAL();
        TerminalNode doubleQuoted = rule.DOUBLE_QUOTED_LITERAL();
        return singleQuoted != null ? singleQuoted : doubleQuoted;
    }
    
    static String getString(TerminalNode rule, Decoder decoder) {
        Token token = rule.getSymbol();
        CharStream cs = token.getInputStream();
        int pos = cs.index();
        cs.seek(token.getStartIndex() + 1);
        int len = token.getStopIndex() - token.getStartIndex() - 1;
        IntSupplier supplier = new IntSupplier() {
            int i = 0;
            
            @Override
            public int getAsInt() {
                if (i >= len) {
                    return -1;
                }
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
                i++;
                return ch;
            }
        };
        String result = decoder.toString(supplier);
        cs.seek(pos);
        return result;
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
        Operator value = rule.expr()!=null ? gen(ctx, planner, rule.expr()) : null;
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
