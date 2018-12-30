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

/**
 * 
 * @author *-xguo0<@
 */
public class Analyzer3 extends Analyzer {
    /*
    boolean analyzeWhere(Planner planner, Operator expr) {
        for (Node node:planner.nodes.values()) {
            analyze(node, expr);
        }
    }

    boolean analyzeJoin(Planner planner, Operator expr, Node scope) {
    }
    
    private boolean analyze(Node node, Operator expr) {
        if (expr instanceof OpAnd) {
            return analyze_and(node, (OpAnd)expr);
        }
        if (expr instanceof OpOr) {
            return analyze_or(node, (OpOr)expr);
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

    private boolean analyze_or(Node node, OpOr expr) {
        analyze(node, expr.left);
        analyze(node, expr.right);
        return false;
    }

    private boolean analyze_and(Node node, OpAnd expr) {
        analyze(node, expr.left);
        analyze(node, expr.right);
        return false;
    }
    */
}
