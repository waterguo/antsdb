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
package com.antsdb.saltedfish.sql.command;

import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.tree.TerminalNode;

import com.antsdb.saltedfish.lexer.FishParser.ColumnContext;
import com.antsdb.saltedfish.lexer.FishParser.ExprContext;
import com.antsdb.saltedfish.lexer.FishParser.Expr_equalContext;
import com.antsdb.saltedfish.lexer.FishParser.LimitContext;
import com.antsdb.saltedfish.lexer.FishParser.ValueContext;
import com.antsdb.saltedfish.lexer.FishParser.WhereContext;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.HColumnRow;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;
import com.antsdb.saltedfish.sql.vdm.FieldValue;
import com.antsdb.saltedfish.sql.vdm.Filter;
import com.antsdb.saltedfish.sql.vdm.HSeek;
import com.antsdb.saltedfish.sql.vdm.HSelect;
import com.antsdb.saltedfish.sql.vdm.Limiter;
import com.antsdb.saltedfish.sql.vdm.LongValue;
import com.antsdb.saltedfish.sql.vdm.NullValue;
import com.antsdb.saltedfish.sql.vdm.OpAnd;
import com.antsdb.saltedfish.sql.vdm.OpEqual;
import com.antsdb.saltedfish.sql.vdm.Operator;
import com.antsdb.saltedfish.sql.vdm.StringValue;

/**
 * 
 * @author *-xguo0<@
 */
class HCrudUtil {
    static GTable getTable(GeneratorContext ctx, int tableId) {
        GTable gtable = ctx.getHumpback().getTable(tableId);
        if (gtable == null) {
            throw new OrcaException("table {} is not found", tableId); 
        }
        return gtable;
    }
    
    static CursorMaker genCursorMaker(GeneratorContext ctx, int tableId, WhereContext where) {
        GTable gtable = getTable(ctx, tableId);
        List<Integer> columns = getColumns(ctx.getHumpback(), gtable);
        if ((columns == null) || (columns.size() == 0)) {
            throw new OrcaException("columns of table {} is not found", tableId);
        }
        HSelect result = hasSeek(where) ? new HSeek(gtable, columns) : new HSelect(gtable, columns);
        return result;
    }

    static List<Integer> getColumns(Humpback humpback, GTable gtable) {
        List<Integer> result = new ArrayList<>();
        result.add(-1);
        result.add(0);
        List<HColumnRow> htableColumns = humpback.getColumns(gtable.getId());
        if ((htableColumns != null) && (htableColumns.size() > 0)) {
            for (HColumnRow i:htableColumns) {
                result.add(new Integer(i.getColumnPos()));
            }
        }
        else {
            int max = 0;
            for (RowIterator i=gtable.scan(0, Long.MAX_VALUE, true);;) {
                if (!i.next()) {
                    break;
                }
                Row row = i.getRow();
                max = Math.max(max, row.getMaxColumnId());
            }
            for (int j=0; j<=max; j++) {
                result.add(new Integer(j));
            }
        }
        return result;
    }

    public static CursorMaker gen(GeneratorContext ctx, int tableId, WhereContext where, LimitContext limit) {
        CursorMaker result = gen(ctx, tableId, where);
        if (limit != null) {
            int limitn = Integer.parseInt(limit.number_value(0).getText());
            int offset = 0;
            if (limit.K_OFFSET() != null) {
                offset = Integer.parseInt(limit.number_value(1).getText());
            }
            result = new Limiter(result, offset, limitn);
        }
        return result;
    }

    static boolean hasSeek(WhereContext rule) {
        if (rule == null) {
            return false;
        }
        return hasSeek(rule.expr());
    }
    
    private static boolean hasSeek(ExprContext rule) {
        if (rule == null) {
            return false;
        }
        if (rule.expr_equal() != null) {
            return isSeek(rule.expr_equal());
        }
        else if (rule.K_AND() != null) {
            return hasSeek(rule.expr(0)) || hasSeek(rule.expr(1));
        }
        return false;
    }

    private static boolean isSeek(Expr_equalContext rule) {
        return rule.column().getText().equals("$00");
    }

    static CursorMaker gen(GeneratorContext ctx, int tableId, WhereContext whereRule) {
        CursorMaker result = genCursorMaker(ctx, tableId, whereRule);
        if (whereRule != null) {
            Operator where = genWhere(result, whereRule);
            result = new Filter(result, where, 1);
        }
        return result;
    }

    static Operator genWhere(CursorMaker maker, WhereContext rule) {
        Operator result = genExpr(maker, rule.expr());
        return result;
    }

    static Operator genExpr(CursorMaker maker, ExprContext rule) {
        Operator result = null;
        if (rule.expr_equal() != null) {
            result = genExprEqual(maker, rule.expr_equal());
        }
        else if (rule.K_AND() != null) {
            result = new OpAnd(genExpr(maker, rule.expr(0)), genExpr(maker, rule.expr(1))); 
        }
        else {
            throw new IllegalArgumentException();
        }
        return result;
    }

    static Operator genExprEqual(CursorMaker maker, Expr_equalContext rule) {
        Operator left = genColumn(maker, rule.column());
        Operator right = genValue(rule.value());
        OpEqual result = new OpEqual(left, right);
        if ((maker instanceof HSeek) && rule.column().getText().equals("$00")) {
            ((HSeek)maker).setKey(right);
        }
        return result;
    }

    static Operator genValue(ValueContext value) {
        Operator result = null;
        if (value.NUMERIC_LITERAL() != null) {
            result = genNumericValue(value.NUMERIC_LITERAL());
        }
        else if (value.STRING_LITERAL() != null) {
            result = genStringValue(value.STRING_LITERAL());
        }
        else if (value.K_NULL() != null) {
            result = new NullValue();
        }
        else {
            throw new IllegalArgumentException();
        }
        return result;
    }

    private static Operator genStringValue(TerminalNode rule) {
        String text = rule.getText();
        text = text.substring(1, text.length()-1);
        Operator result = new StringValue(text);
        return result;
    }

    private static Operator genNumericValue(TerminalNode rule) {
        Operator result = new LongValue(Long.parseLong(rule.getText()));
        return result;
    }

    static FieldValue genColumn(CursorMaker maker, ColumnContext column) {
        CursorMeta meta = maker.getCursorMeta();
        String text = column.getText();
        text = text.substring(1, text.length());
        for (int i=0; i<meta.getColumns().size(); i++) {
            FieldMeta ii = meta.getColumn(i);
            if (ii.getName().equals(text)) {
                return new FieldValue(ii, i);
            }
        }
        throw new OrcaException("column {} is not found", text);
    }

}
