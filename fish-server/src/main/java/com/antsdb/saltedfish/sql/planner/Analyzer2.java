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

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import com.antsdb.saltedfish.sql.vdm.BinaryOperator;
import com.antsdb.saltedfish.sql.vdm.BooleanValue;
import com.antsdb.saltedfish.sql.vdm.FieldValue;
import com.antsdb.saltedfish.sql.vdm.NullValue;
import com.antsdb.saltedfish.sql.vdm.OpAnd;
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
 * 
 * @author *-xguo0<@
 */
class Analyzer2 {
    private List<Join> joins;
    private Collection<Node> nodes;

    Analyzer2(Collection<Node> nodes) {
        this.nodes = nodes;
    }
    
    List<Join> getResult() {
        return this.joins;
    }
    
    boolean analyzeJoin(Operator expr, Node scope) {
        return analyze(expr, scope);
    }
    
    boolean analyzeWhere(Operator expr) {
        return analyze(expr, null);
    }
    
    private boolean analyze(Operator expr, Node scope) {
        // create the first join if none exist
        
        if (this.joins == null) {
            this.joins = new LinkedList<>();
            Join join = new Join();
            this.joins.add(join);
            for (Node i:nodes) {
                QueryNode node = new QueryNode();
                node.node = i;
                join.nodes.add(node);
            }
        }
        
        // push down the condition to node level across all joins
        
        boolean result = analyze(joins, expr, scope);

        // keep the remaining condition
        
        for (Join i:this.joins) {
            if (result) {
                // condition is fully pushed down
                continue;
            }
            if (scope == null) {
                // this is a where condition
                if (i.where != null) {
                    throw new IllegalArgumentException();
                }
                i.where = expr;
            }
            else {
                // this is a join condition
                for (QueryNode j:i.nodes) {
                    if (j.node != scope) {
                        continue;
                    }
                    if (j.condition != null) {
                        throw new IllegalArgumentException();
                    }
                    j.condition = expr;
                }
            }
        }
        return result;
    }
    
    private boolean analyze(List<Join> joins, Operator expr, Node scope) {
        if (expr instanceof OpAnd) {
            return analyze_and(joins, (OpAnd) expr, scope);
        }
        if (expr instanceof OpOr) {
            return analyze_or(joins, (OpOr) expr, scope);
        }
        else if (expr instanceof BinaryOperator) {
            return analyze_binary(joins, (BinaryOperator) expr, scope);
        }
        else if (expr instanceof OpIsNull) {
            Operator upstream = ((OpIsNull) expr).getUpstream();
            if (upstream instanceof FieldValue) {
                FieldValue field = (FieldValue) upstream;
                return analyze_binary(joins, FilterOp.EQUALNULL, field, new NullValue(), expr, scope);
            }
        }
        else if (expr instanceof OpMatch) {
            OpMatch match = (OpMatch) expr;
            ColumnFilter cf = new ColumnFilter(null, FilterOp.MATCH, match, null, scope!=null);
            Node node = ((OpMatch) expr).getColumns().get(0).getField().owner;
            addFilter(joins, node, cf);
            return true;
        }
        return false;
    }
    
    private boolean analyze_and(List<Join> joins, OpAnd op, Node scope) {
        boolean resultFromLeft = analyze(joins, op.left, scope);
        if (resultFromLeft) {
            op.left = new BooleanValue(true);
        }
        boolean resultFromRight = analyze(joins, op.right, scope);
        if (resultFromRight) {
            op.right = new BooleanValue(true);
        }
        return resultFromLeft && resultFromRight;
    }
    
    private boolean analyze_or(List<Join> joins, OpOr op, Node scope) {
        List<Join> copy = new LinkedList<>();
        for (Join i:joins) {
            Join ii = i.clone();
            copy.add(ii);
        }
        boolean resultFromLeft = analyze(joins, op.left, scope);
        if (resultFromLeft) {
            op.left = new BooleanValue(true);
        }
        boolean resultFromRight = analyze(copy, op.right, scope);
        if (resultFromRight) {
            op.right = new BooleanValue(true);
        }
        this.joins.addAll(copy);
        return resultFromLeft && resultFromRight;
    }

    private boolean analyze_binary(List<Join> joins, BinaryOperator op, Node scope) {
        boolean result = false;
        if (op.left instanceof FieldValue) {
            FieldValue field = (FieldValue) op.left;
            if (op instanceof OpEqual) {
                result = analyze_binary(joins, FilterOp.EQUAL, field, op.right, op, scope);
            }
            else if (op instanceof OpEqualNull) {
                result = analyze_binary(joins, FilterOp.EQUALNULL, field, op.right, op, scope);
            }
            else if (op instanceof OpLarger) {
                result = analyze_binary(joins, FilterOp.LARGER, field, op.right, op, scope);
            }
            else if (op instanceof OpLargerEqual) {
                result = analyze_binary(joins, FilterOp.LARGEREQUAL, field, op.right, op, scope);
            }
            else if (op instanceof OpLess) {
                result = analyze_binary(joins, FilterOp.LESS, field, op.right, op, scope);
            }
            else if (op instanceof OpLessEqual) {
                result = analyze_binary(joins, FilterOp.LESSEQUAL, field, op.right, op, scope);
            }
            else if (op instanceof OpLike) {
                result = analyze_binary(joins, FilterOp.LIKE, field, op.right, op, scope);
            }
            else if (op instanceof OpInSelect) {
                result = analyze_binary(joins, FilterOp.INSELECT, field, op, op, scope);
            }
            else if (op instanceof OpInValues) {
                result = analyze_binary(joins, FilterOp.INVALUES, field, op, op, scope);
            }
        }
        if (op.right instanceof FieldValue) {
            FieldValue field = (FieldValue) op.right;
            if (op instanceof OpEqual) {
                result = result | analyze_binary(joins, FilterOp.EQUAL, field, op.left, op, scope);
            }
            else if (op instanceof OpEqualNull) {
                result = result | analyze_binary(joins, FilterOp.EQUALNULL, field, op.left, op, scope);
            }
            else if (op instanceof OpLarger) {
                result = result | analyze_binary(joins, FilterOp.LESSEQUAL, field, op.left, op, scope);
            }
            else if (op instanceof OpLargerEqual) {
                result = result | analyze_binary(joins, FilterOp.LESS, field, op.left, op, scope);
            }
            else if (op instanceof OpLess) {
                result = result | analyze_binary(joins, FilterOp.LARGEREQUAL, field, op.left, op, scope);
            }
            else if (op instanceof OpLessEqual) {
                result = result | analyze_binary(joins, FilterOp.LARGER, field, op.left, op, scope);
            }
        }
        return result;
    }

    private boolean analyze_binary(
            List<Join> joins, 
            FilterOp op, 
            FieldValue field, 
            Operator value,
            Operator source, 
            Node scope) {
        // the whole purpose of analysis is to translate condition to column
        // filter. it is right here. be aware that
        // only the filter that fits in the scope can be added.

        PlannerField fieldLeft = (PlannerField) field.getField();
        ColumnFilter cf = null;
        if (fieldLeft.owner == scope) {
            cf = new ColumnFilter(fieldLeft, op, value, source, scope!=null);
        }
        else if ((scope == null) && (this.nodes.contains(fieldLeft.owner))) {
            // don't touch parent planner.
            cf = new ColumnFilter(fieldLeft, op, value, source, scope!=null);
        }
        else {
            // false means the specified condition cannot be translated to a column
            // filter. thus we should keep it where
            // it is
            return false;
        }
        cf.isConstant = isConstant(fieldLeft.owner, value);
        addFilter(joins, fieldLeft.owner, cf);
        return true;
    }

    private void addFilter(List<Join> joins, Node n, ColumnFilter cf) {
        for (Join join:joins) {
            QueryNode node = join.getQueryNode(n);
            addFilter(node, cf);
        }
    }
    
    /**
     * if expr calculatable in the scope of node
     * 
     * @param to
     * @param expr
     * @return
     */
    static boolean isConstant(Node node, Operator expr) {
        boolean[] valid = new boolean[1];
        valid[0] = true;
        expr.visit(it -> {
            if (!valid[0]) {
                return;
            }
            if (it instanceof FieldValue) {
                FieldValue cv = (FieldValue) it;
                if (cv.getField().owner != node) {
                    valid[0] = false;
                }
            }
        });
        return valid[0];
    }
    
    private void addFilter(QueryNode node, ColumnFilter cf) {
        node.addFilter(cf);
    }
}
