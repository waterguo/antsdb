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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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
    static final Node INVALID_NODE = new Node(); 
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
        init(planner);
        return analyze_(0, this.version, planner, expr, null);
    }

    boolean analyzeJoin(Planner planner, Operator expr, Node scope) {
        init(planner);
        return analyze_(0, this.version, planner, expr, scope);
    }

    private void init(Planner planner) {
        // make sure each node starts with a full row set
        for (Node i:planner.nodes.values()) {
            if (i.union.size() == 0) {
                i.union.add(new RowSet(this.version));
            }
        }
    }
    
    private boolean analyze_(int start, int end, Planner planner, Operator expr, Node scope) {
        if (expr instanceof OpAnd) {
            return analyze_and(start, end, planner, (OpAnd) expr, scope);
        }
        if (expr instanceof OpOr) {
            return analyze_or(start, end, planner, (OpOr) expr, scope);
        }
        else if (expr instanceof BinaryOperator) {
            return analyze_binary(start, end, planner, (BinaryOperator) expr, scope);
        }
        else if (expr instanceof OpBetween) {
            OpBetween between = (OpBetween)expr;
            Operator upstream = between.getLeftOperator();
            if (upstream instanceof FieldValue) {
                FieldValue field = (FieldValue) upstream;
                if (!analyze_binary(start, end, planner, FilterOp.LARGEREQUAL, field, between.getFrom(), expr, scope)) {
                    return false;
                }
                if (!analyze_binary(start, end, planner, FilterOp.LESSEQUAL, field, between.getTo(), expr, scope)) {
                    return false;
                }
                return true;
            }
        }
        else if (expr instanceof OpIsNull) {
            Operator upstream = ((OpIsNull) expr).getUpstream();
            if (upstream instanceof FieldValue) {
                FieldValue field = (FieldValue) upstream;
                return analyze_binary(start, end, planner, FilterOp.EQUALNULL, field, new NullValue(), expr, scope);
            }
        }
        else if (expr instanceof OpMatch) {
            OpMatch match = (OpMatch) expr;
            ColumnFilter cf = new ColumnFilter(null, FilterOp.MATCH, match, null, scope!=null);
            Node node = ((OpMatch) expr).getColumns().get(0).getField().owner;
            addFilter(start, end, node, cf);
            return true;
        }
        else {
            return analyze_other(start, end, scope, expr);
        }
        return false;
    }

    /*
     * x + (y * z) = x + y * z = x, y * z
     * x * (y * z) = x * y * z
     */
    private boolean analyze_and(int start, int end, Planner planner, OpAnd op, Node scope) {
        boolean resultFromLeft = analyze_(start, end, planner, op.left, scope);
        if (resultFromLeft) {
            op.left = new BooleanValue(true);
        }
        // new RowSet might have been added when analyzing op.left. we need to include potential new RowSet
        boolean resultFromRight = analyze_(start, this.version, planner, op.right, scope);
        if (resultFromRight) {
            op.right = new BooleanValue(true);
        }
        return resultFromLeft && resultFromRight;
    }

    /*
     * x * (y + z) = x * y + x * z = x * y, x * z
     * x + (y + z) = x, y, z
     */
    private boolean analyze_or(int start, int end, Planner planner, OpOr op, Node scope) {
        // we only want to do it if we completely understand the conditions
        if (!canPushDown(planner, op, scope)) {
            return false;
        }
        
        // find the RowSets and keep a copy
        Node node = findNodeInOr(op);
        if (node == null) {
            return false;
        }
        List<RowSet> x = new LinkedList<>();
        findSets(node, start, end, rs->{
            RowSet clone = new RowSet();
            clone.conditions.addAll(rs.conditions);
            x.add(clone);
        });
        
        // run through the conditions on the left side
        if (!analyze_(start, end, planner, op.left, scope)) {
            throw new IllegalArgumentException();
        }
        op.left = new BooleanValue(true);
        
        // add the copies. we do it this awkward way cuz we want the version to be continues during analyze_and 
        this.version++;
        x.forEach(it->it.tag = this.version);
        node.union.addAll(x);
        
        // run through the conditions on the right side
        if (!analyze_(this.version, this.version, planner, op.right, scope)) {
            throw new IllegalArgumentException();
        }
        op.right = new BooleanValue(true);
        return true;
    }

    /**
<<<<<<< HEAD
     * find the node inside an OR operator. conditions can only be pushed down when there is only one node
=======
     * find the node inside an expression. this is identical to findNodeInOr except return value
>>>>>>> wg_audi
     * involved. return null if there are more nodes involved 
     * 
     * @param op
     * @return null if input has no node involved; INVALID_NODE if multiple nodes are involved; otherwise
     * return the only node involved in the expression 
     */
    private Node findNodeInExpresion(Operator op) {
        AtomicReference<Node> result = new AtomicReference<>();
        op.visit((Operator it)->{
            if (it instanceof FieldValue) {
                Node node = ((FieldValue)it).getField().owner;
                if (result.get() == null) {
                    result.set(node);
                }
                else if (node != result.get()) {
                    result.set(INVALID_NODE);
                }
            }
        });
        return result.get();
    }
    
    /**
     * find the node inside an OR operator. conditions can only be pushed down when there is ony one node
     * involved. return null if there are more nodes involved 
     * 
     * @param op
     * @return
     */
    private Node findNodeInOr(Operator op) {
        Node result = findNodeInExpresion(op);
        return result == INVALID_NODE ? null : result;
    }

    private boolean analyze_binary(int start, int end, Planner planner, BinaryOperator op, Node scope) {
        boolean result = false;
        if (op.left instanceof FieldValue) {
            FieldValue field = (FieldValue) op.left;
            if (op instanceof OpEqual) {
                result = analyze_binary(start, end, planner, FilterOp.EQUAL, field, op.right, op, scope);
            }
            else if (op instanceof OpEqualNull) {
                result = analyze_binary(start, end, planner, FilterOp.EQUALNULL, field, op.right, op, scope);
            }
            else if (op instanceof OpLarger) {
                result = analyze_binary(start, end, planner, FilterOp.LARGER, field, op.right, op, scope);
            }
            else if (op instanceof OpLargerEqual) {
                result = analyze_binary(start, end, planner, FilterOp.LARGEREQUAL, field, op.right, op, scope);
            }
            else if (op instanceof OpLess) {
                result = analyze_binary(start, end, planner, FilterOp.LESS, field, op.right, op, scope);
            }
            else if (op instanceof OpLessEqual) {
                result = analyze_binary(start, end, planner, FilterOp.LESSEQUAL, field, op.right, op, scope);
            }
            else if (op instanceof OpLike) {
                result = analyze_binary(start, end, planner, FilterOp.LIKE, field, op.right, op, scope);
            }
            else if (op instanceof OpInSelect) {
                OpInSelect in = (OpInSelect)op;
                result = analyze_binary(start, end, planner, FilterOp.INSELECT, field, in.getSelect(), op, scope);
            }
            else if (op instanceof OpInValues) {
                OpInValues in = (OpInValues)op;
                result = analyze_binary(start, end, planner, FilterOp.INVALUES, field, in.getValues(), op, scope);
            }
        }
        if (op.right instanceof FieldValue) {
            FieldValue field = (FieldValue) op.right;
            if (op instanceof OpEqual) {
                result = result | analyze_binary(start, end, planner, FilterOp.EQUAL, field, op.left, op, scope);
            }
            else if (op instanceof OpEqualNull) {
                result = result | analyze_binary(start, end, planner, FilterOp.EQUALNULL, field, op.left, op, scope);
            }
            else if (op instanceof OpLarger) {
                result = result | analyze_binary(start, end, planner, FilterOp.LESSEQUAL, field, op.left, op, scope);
            }
            else if (op instanceof OpLargerEqual) {
                result = result | analyze_binary(start, end, planner, FilterOp.LESS, field, op.left, op, scope);
            }
            else if (op instanceof OpLess) {
                result = result | analyze_binary(start, end, planner, FilterOp.LARGEREQUAL, field, op.left, op, scope);
            }
            else if (op instanceof OpLessEqual) {
                result = result | analyze_binary(start, end, planner, FilterOp.LARGER, field, op.left, op, scope);
            }
        }
        if (!result) {
            result = analyze_other(start, end, scope, op);
        }
        return result;
    }

    /*
     * check if this expression can be pushed down. as long as the expression contains FieldValue coming from
     * one or less nodes, it can.
     */
    private boolean analyze_other(int start, int end, Node scope, Operator op) {
        Node node = findNodeInExpresion(op);
        if (node == null) return false;
        if (scope == null || node == scope) {
            ColumnFilter cf = new ColumnFilter(null, FilterOp.OTHER, op, op, scope!=null);
            cf.isConstant = true;
            addFilter(start, end, node, cf);
            return true;
        }
        else {
            return false;
        }
    }

    private boolean analyze_binary(
            int start,
            int end,
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
            cf = new ColumnFilter(fieldLeft, op, value, source, scope!=null);
        }
        else if ((scope == null) && (planner.nodes.containsValue(node))) {
            // don't touch parent planner.
            cf = new ColumnFilter(fieldLeft, op, value, source, scope!=null);
        }
        else {
            return false;
        }

        // false means the specified condition cannot be translated to a column
        // filter. thus we should keep it where
        // it is
        cf.isConstant = isConstant(node, value);
        addFilter(start, end, node, cf);
        return true;
    }

    private void addFilter(int start, int end, Node node, ColumnFilter cf) {
        if (!findSets(node, start, end, rs->rs.add(cf))) {
            throw new IllegalArgumentException(cf.toString());
        }
    }

    private boolean findSets(Node node, int start, int end, Consumer<RowSet> func) {
        boolean found = false;
        for (RowSet i:node.union) {
            if ((i.tag >= start) && (i.tag <= end)) {
                func.accept(i);
                found = true;
            }
        }
        return found;
    }
    /*
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
    */

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
        if (hasJoin(op)) {
            // can push down the expression if OR implies a join. for example
            // SELECT * FROM x, y WHERE x.id=1 OR y.id=2
            // the result should be the super set of x(x.id=1) -> y  +  x->y(y.id=2)
            return false;
        }
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

    private boolean hasJoin(OpOr op) {
        AtomicBoolean foundJoin = new AtomicBoolean(false);
        Consumer<Operator> call = new Consumer<Operator>() {
            Node found;
            @Override
            public void accept(Operator it) {
                if (foundJoin.get()) {
                    return;
                }
                if (it instanceof FieldValue) {
                    FieldValue fv = (FieldValue)it;
                    Node owner = fv.getField().owner;
                    if (found == null) {
                        found = owner;
                    }
                    else if (found != owner) {
                        foundJoin.set(true);
                    }
                }
            }
        };
        op.visit(call);
        return foundJoin.get();
    }

}
