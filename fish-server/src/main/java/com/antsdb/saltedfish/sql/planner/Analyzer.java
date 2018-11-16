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
package com.antsdb.saltedfish.sql.planner;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.commons.lang.NotImplementedException;

import com.antsdb.saltedfish.sql.vdm.BinaryOperator;
import com.antsdb.saltedfish.sql.vdm.BooleanValue;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.FieldValue;
import com.antsdb.saltedfish.sql.vdm.NullValue;
import com.antsdb.saltedfish.sql.vdm.OpAnd;
import com.antsdb.saltedfish.sql.vdm.OpBetween;
import com.antsdb.saltedfish.sql.vdm.OpEqual;
import com.antsdb.saltedfish.sql.vdm.OpEqualNull;
import com.antsdb.saltedfish.sql.vdm.OpInSelect;
import com.antsdb.saltedfish.sql.vdm.OpInValues;
import com.antsdb.saltedfish.sql.vdm.OpIsNull;
import com.antsdb.saltedfish.sql.vdm.OpLarger;
import com.antsdb.saltedfish.sql.vdm.OpLargerEqual;
import com.antsdb.saltedfish.sql.vdm.OpLess;
import com.antsdb.saltedfish.sql.vdm.OpLessEqual;
import com.antsdb.saltedfish.sql.vdm.OpLike;
import com.antsdb.saltedfish.sql.vdm.OpMatch;
import com.antsdb.saltedfish.sql.vdm.OpOr;
import com.antsdb.saltedfish.sql.vdm.Operator;

/**
 * rewrites the query
 * 
 * @author wgu0
 */
class Analyzer {
    int version = 0;
    
    enum Mode {
        AND,
        OR,
        COPY_OR,
    }
    
    /**
     * 
     * @param planner
     * @param expr
     * @param scope limit the analyzer to the specified scope. it is needed when analyzing join condition
     * @return
     */
    boolean analyzeWhere(Planner planner, Operator expr) {
        int set = makeSet(0, this.version);
        return analyze_(Mode.AND, set, planner, expr, null);
    }

    boolean analyzeJoin(Planner planner, Operator expr, Node scope) {
        int set = makeSet(0, this.version);
        return analyze_(Mode.AND, set, planner, expr, scope);
    }
    
    boolean analyze_(Mode mode, int set, Planner planner, Operator expr, Node scope) {
        if (expr instanceof OpAnd) {
            return analyze_and(mode, set, planner, (OpAnd) expr, scope);
        }
        if (expr instanceof OpOr) {
            return analyze_or(mode, set, planner, (OpOr) expr, scope);
        }
        else if (expr instanceof BinaryOperator) {
            return analyze_binary(mode, set, planner, (BinaryOperator) expr, scope);
        }
        else if (expr instanceof OpBetween) {
            OpBetween between = (OpBetween)expr;
            Operator upstream = between.getLeftOperator();
            if (upstream instanceof FieldValue) {
                FieldValue field = (FieldValue) upstream;
                int ystart = this.version;
                if (!analyze_binary(mode, set, planner, FilterOp.LARGEREQUAL, field, between.getFrom(), expr, scope)) {
                    return false;
                }
                int y = makeSet(ystart, this.version);
                if (!analyze_binary(Mode.AND, y, planner, FilterOp.LESSEQUAL, field, between.getTo(), expr, scope)) {
                    return false;
                }
                return true;
            }
        }
        else if (expr instanceof OpIsNull) {
            Operator upstream = ((OpIsNull) expr).getUpstream();
            if (upstream instanceof FieldValue) {
                FieldValue field = (FieldValue) upstream;
                return analyze_binary(mode, set, planner, FilterOp.EQUALNULL, field, new NullValue(), expr, scope);
            }
        }
        else if (expr instanceof OpMatch) {
            OpMatch match = (OpMatch) expr;
            ColumnFilter cf = new ColumnFilter(null, FilterOp.MATCH, match, null);
            Node node = ((OpMatch) expr).getColumns().get(0).getField().owner;
            addFilter(mode, set, node, cf);
            return true;
        }
        return false;
    }

    /*
     * x + (y * z) = x + y * z = x, y * z
     * x * (y * z) = x * y * z
     */
    private boolean analyze_and(Mode mode, int x, Planner planner, OpAnd op, Node scope) {
        this.version++;
        int ystart = this.version;
        boolean resultFromLeft = analyze_(mode, x, planner, op.left, scope);
        int y = makeSet(ystart, this.version);
        if (resultFromLeft) {
            op.left = new BooleanValue(true);
            boolean resultFromRight = analyze_(Mode.AND, y, planner, op.right, scope);
            if (resultFromRight) {
                op.right = new BooleanValue(true);
            }
            return resultFromRight;
        }
        else {
            analyze_(mode, x, planner, op.right, scope);
            return false;
        }
    }

    /*
     * x * (y + z) = x * y + x * z = x * y, x * z
     * x + (y + z) = x, y, z
     */
    boolean analyze_or(Mode mode, int x, Planner planner, OpOr op, Node scope) {
        if (!canPushDown(planner, op, scope)) {
            return false;
        }
        this.version++;
        if (!analyze_(mode, x, planner, op.left, scope)) {
            throw new IllegalArgumentException();
        }
        op.left = new BooleanValue(true);
        this.version++;
        if (!analyze_(Mode.OR, x, planner, op.right, scope)) {
            throw new IllegalArgumentException();
        }
        op.right = new BooleanValue(true);
        return true;
    }

    private boolean analyze_binary(Mode mode, int set, Planner planner, BinaryOperator op, Node scope) {
        boolean result = false;
        if (op.left instanceof FieldValue) {
            FieldValue field = (FieldValue) op.left;
            if (op instanceof OpEqual) {
                result = analyze_binary(mode, set, planner, FilterOp.EQUAL, field, op.right, op, scope);
            }
            else if (op instanceof OpEqualNull) {
                result = analyze_binary(mode, set, planner, FilterOp.EQUALNULL, field, op.right, op, scope);
            }
            else if (op instanceof OpLarger) {
                result = analyze_binary(mode, set, planner, FilterOp.LARGER, field, op.right, op, scope);
            }
            else if (op instanceof OpLargerEqual) {
                result = analyze_binary(mode, set, planner, FilterOp.LARGEREQUAL, field, op.right, op, scope);
            }
            else if (op instanceof OpLess) {
                result = analyze_binary(mode, set, planner, FilterOp.LESS, field, op.right, op, scope);
            }
            else if (op instanceof OpLessEqual) {
                result = analyze_binary(mode, set, planner, FilterOp.LESSEQUAL, field, op.right, op, scope);
            }
            else if (op instanceof OpLike) {
                result = analyze_binary(mode, set, planner, FilterOp.LIKE, field, op.right, op, scope);
            }
            else if (op instanceof OpInSelect) {
                OpInSelect in = (OpInSelect)op;
                result = analyze_binary(mode, set, planner, FilterOp.INSELECT, field, in.getSelect(), op, scope);
            }
            else if (op instanceof OpInValues) {
                OpInValues in = (OpInValues)op;
                result = analyze_binary(mode, set, planner, FilterOp.INVALUES, field, in.getValues(), op, scope);
            }
        }
        if (op.right instanceof FieldValue) {
            FieldValue field = (FieldValue) op.right;
            if (op instanceof OpEqual) {
                result = result | analyze_binary(mode, set, planner, FilterOp.EQUAL, field, op.left, op, scope);
            }
            else if (op instanceof OpEqualNull) {
                result = result | analyze_binary(mode, set, planner, FilterOp.EQUALNULL, field, op.left, op, scope);
            }
            else if (op instanceof OpLarger) {
                result = result | analyze_binary(mode, set, planner, FilterOp.LESSEQUAL, field, op.left, op, scope);
            }
            else if (op instanceof OpLargerEqual) {
                result = result | analyze_binary(mode, set, planner, FilterOp.LESS, field, op.left, op, scope);
            }
            else if (op instanceof OpLess) {
                result = result | analyze_binary(mode, set, planner, FilterOp.LARGEREQUAL, field, op.left, op, scope);
            }
            else if (op instanceof OpLessEqual) {
                result = result | analyze_binary(mode, set, planner, FilterOp.LARGER, field, op.left, op, scope);
            }
        }
        return result;
    }

    private boolean analyze_binary(
            Mode mode,
            int set,
            Planner planner, 
            FilterOp op, 
            FieldValue field, 
            Object value,
            Operator source, 
            Node scope) {
        // the whole purpose of analysis is to translate condition to column
        // filter. it is right here. be aware that
        // only the filter that fits in the scope can be added.

        PlannerField fieldLeft = (PlannerField) field.getField();
        Node node = fieldLeft.owner;
        ColumnFilter cf = null;
        if (node == scope) {
            cf = new ColumnFilter(fieldLeft, op, value, source);
        }
        else if ((scope == null) && (planner.nodes.containsValue(node))) {
            // don't touch parent planner.
            cf = new ColumnFilter(fieldLeft, op, value, source);
        }
        else {
            return false;
        }

        // false means the specified condition cannot be translated to a column
        // filter. thus we should keep it where
        // it is

        cf.isConstant = isConstant(node, value);
        addFilter(mode, set, node, cf);
        return true;
    }

    private void addFilter(Mode mode, int set, Node node, ColumnFilter cf) {
        cf.version = this.version;
        if (mode == Mode.AND) {
            if (node.union.size() == 0) {
                List<ColumnFilter> list = new LinkedList<>();
                list.add(cf);
                node.union.add(list);
            }
            else {
                iterateSet(node, set, it -> {
                    it.add(cf);
                });
            }
        }
        else if (mode == Mode.OR) {
            List<ColumnFilter> list = new LinkedList<>();
            list.add(cf);
            node.union.add(list);
        }
        else if (mode == Mode.COPY_OR) {
            List<List<ColumnFilter>> list = new ArrayList<>();
            iterateSet(node, set, it -> {
                List<ColumnFilter> clone = subset(it, set);
                clone.add(cf);
                list.add(clone);
            });
            if (list.isEmpty()) {
                List<ColumnFilter> clone = new LinkedList<>();
                clone.add(cf);
                list.add(clone);
            }
            node.union.addAll(list);
        }
        else {
            throw new IllegalArgumentException();
        }
    }

    private List<ColumnFilter> subset(List<ColumnFilter> input, int set) {
        List<ColumnFilter> result = new ArrayList<>();
        int start = set >>> 16;
        int end = set & 0xffff;
        for (ColumnFilter j:input) {
            if ((j.version >= start) && (j.version <= end)) {
                result.add(j);
            }
        }
        return result;
    }

    /**
     * if expr calculatable in the scope of node
     * 
     * @param to
     * @param expr
     * @return
     */
    static boolean isConstant(Node node, Object expr) {
        if (expr instanceof CursorMaker) {
            return false;
        }
        else if (expr instanceof List<?>) {
            for (Object i:(List<?>)expr) {
                if (!isConstant(node, i)) {
                    return false;
                }
            }
            return true;
        }
        else if (expr instanceof Operator) {
            AtomicBoolean valid = new AtomicBoolean(true);
            ((Operator)expr).visit(it -> {
                if (!valid.get()) {
                    return;
                }
                if (it instanceof FieldValue) {
                    FieldValue cv = (FieldValue) it;
                    if (cv.getField().owner != node) {
                        valid.set(false);
                    }
                }
            });
            return valid.get();
        }
        else {
            throw new NotImplementedException();
        }
    }
    
    private static int makeSet(int start, int end) {
        int result = start << 16 | (end & 0xffff);
        return result;
    }
    
    private void iterateSet(Node node, int set, Consumer<List<ColumnFilter>> func) {
        int start = set >>> 16;
        int end = set & 0xffff;
        for (List<ColumnFilter> i:node.union) {
            for (ColumnFilter j:i) {
                if ((j.version >= start) && (j.version <= end)) {
                    func.accept(i);
                    break;
                }
            }
        }
    }

    private boolean canPushDown(Planner planner, Operator op, Node scope) {
        boolean result = false;
        if (op instanceof OpOr) {
            result = canPushDown(planner, (OpOr)op, scope);
        }
        else if (op instanceof OpAnd) {
            result = canPushDown(planner, (OpAnd)op, scope);
        }
        else if (op instanceof BinaryOperator) {
            result = canPushDown(planner, (BinaryOperator)op, scope);
        }
        else if (op instanceof OpBetween) {
            result = canPushDown(planner, (OpBetween)op, scope);
        }
        else if (op instanceof OpIsNull) {
            result = canPushDown(planner, (OpIsNull)op, scope);
        }
        else if (op instanceof OpMatch) {
            result = canPushDown(planner, (OpMatch)op, scope);
        }
        return result;
    }

    private boolean canPushDown(Planner planner, OpOr op, Node scope) {
        return canPushDown(planner, op.left, scope) && canPushDown(planner, op.right, scope);
    }
    
    private boolean canPushDown(Planner planner, OpAnd op, Node scope) {
        return canPushDown(planner, op.left, scope) && canPushDown(planner, op.right, scope);
    }
    
    private boolean canPushDown(Planner planner, BinaryOperator op, Node scope) {
        FieldValue fv;
        
        // one of the operand must be FieldValue
        
        if (op.left instanceof FieldValue) {
            fv = (FieldValue)op.left;
        }
        else if (op.right instanceof FieldValue) {
            fv = (FieldValue)op.right;
        }
        else {
            return false;
        }
        
        // validate the operator 
        
        if (op instanceof OpEqual) {
        }
        else if (op instanceof OpEqualNull) {
        }
        else if (op instanceof OpLarger) {
        }
        else if (op instanceof OpLargerEqual) {
        }
        else if (op instanceof OpLess) {
        }
        else if (op instanceof OpLessEqual) {
        }
        else if (op instanceof OpLike) {
        }
        else if (op instanceof OpInSelect) {
        }
        else if (op instanceof OpInValues) {
        }
        else {
            return false;
        }
        
        // validate the scope
        
        PlannerField pf = fv.getField();
        Node node = pf.owner;
        if (node == scope) {
            return true;
        }
        else if ((scope == null) && (planner.nodes.containsValue(node))) {
            return true;
        }
        else {
            return false;
        }
    }
    
    private boolean canPushDown(Planner planner, OpBetween op, Node scope) {
        return op.getLeftOperator() instanceof FieldValue;
    }
    
    private boolean canPushDown(Planner planner, OpIsNull op, Node scope) {
        return op.getUpstream() instanceof FieldValue; 
    }

    private boolean canPushDown(Planner planner, OpMatch op, Node scope) {
        return true;
    }

}
