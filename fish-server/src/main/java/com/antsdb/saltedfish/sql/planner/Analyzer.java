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
package com.antsdb.saltedfish.sql.planner;

import java.util.ArrayList;

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
 * rewrites the query
 *  
 * @author wgu0
 */
class Analyzer {
	static boolean analyze(Planner planner, Operator expr, Node scope) {
		if (planner.nodes.size() != 1) {
			// only one node for now
			return analyze_(planner, expr, scope);
		}
		if (!(expr instanceof OpOr)) {
			// only top level OR for now
			return analyze_(planner, expr, scope);
		}
		
		// deal with OR logic
		
		OpOr or = (OpOr)expr;
		Node parent = planner.nodes.values().iterator().next();
		if (parent.table == null) {
			return analyze_(planner, expr, scope);
		}
		parent.unions = new ArrayList<>();
		Node left = createChildNode(parent);
		parent.unions.add(left);
		parent.setActive(left);
		boolean resultFromLeft = analyze_(planner, or.left, null);
		if (resultFromLeft) {
			or.left = new BooleanValue(true);
		}
		left.where = or.left;
		Node right = createChildNode(parent);
		parent.unions.add(right);
		parent.setActive(right);
		boolean resultFromRight = analyze_(planner, or.right, null);
		if (resultFromRight) {
			or.right = new BooleanValue(true);
		}
		right.where = or.right;
		//parent.getFilters().addAll(left.getFilters());
		//parent.getFilters().addAll(right.getFilters());
		return true;
	}
	
	static Node createChildNode(Node parent) {
		Node node = new Node();
		node.maker = parent.maker;
		node.table = parent.table;
		node.fields = parent.fields;
		return node;
	}
	
	static boolean analyze_(Planner planner, Operator expr, Node scope) {
        if (expr instanceof OpAnd) {
            return analyze_and(planner, (OpAnd)expr, scope);
        }
        else if (expr instanceof BinaryOperator) {
            return analyze_binary(planner, (BinaryOperator)expr, scope);
        }
        else if (expr instanceof OpIsNull) {
        	Operator upstream = ((OpIsNull)expr).getUpstream();
            if (upstream instanceof FieldValue) {
            	FieldValue field = (FieldValue)upstream;
            	return analyze_binary(planner, FilterOp.EQUALNULL, field, new NullValue(), expr, scope);
            }
        }
        else if (expr instanceof OpMatch) {
        	OpMatch match = (OpMatch)expr;
        	ColumnFilter cf = new ColumnFilter(null, FilterOp.MATCH, match, null);
        	Node node = ((OpMatch) expr).getColumns().get(0).getField().owner;
        	node.addFilter(cf);
        	return true;
        }
        return false;
	}

	static void analyzeOr(Planner planner, Operator expr) {
	}
	
    private static boolean analyze_and(Planner planner, OpAnd op, Node scope) {
        boolean resultFromLeft = analyze_(planner, op.left, scope);
        if (resultFromLeft) {
        	op.left = new BooleanValue(true);
        }
        boolean resultFromRight = analyze_(planner, op.right, scope);
        if (resultFromRight) {
        	op.right = new BooleanValue(true);
        }
        return resultFromLeft && resultFromRight;
    }

    private static boolean analyze_binary(Planner planner, BinaryOperator op, Node scope) {
    	boolean result = false;
        if (op.left instanceof FieldValue) {
        	FieldValue field = (FieldValue)op.left;
            if (op instanceof OpEqual) {
            	result = analyze_binary(planner, FilterOp.EQUAL, field, op.right, op, scope);
            }
            else if (op instanceof OpEqualNull) {
            	result = analyze_binary(planner, FilterOp.EQUALNULL, field, op.right, op, scope);
            }
            else if (op instanceof OpLarger) {
            	result = analyze_binary(planner, FilterOp.LARGER, field, op.right, op, scope);
            }
            else if (op instanceof OpLargerEqual) {
            	result = analyze_binary(planner, FilterOp.LARGEREQUAL, field, op.right, op, scope);
            }
            else if (op instanceof OpLess) {
            	result = analyze_binary(planner, FilterOp.LESS, field, op.right, op, scope);
            }
            else if (op instanceof OpLessEqual) {
            	result = analyze_binary(planner, FilterOp.LESSEQUAL, field, op.right, op, scope);
            }
            else if (op instanceof OpLike) {
            	result = analyze_binary(planner, FilterOp.LIKE, field, op.right, op, scope);
            }
            else if (op instanceof OpInSelect) {
            	result = analyze_binary(planner, FilterOp.INSELECT, field, op, op, scope);
            }
            else if (op instanceof OpInValues) {
            	result = analyze_binary(planner, FilterOp.INVALUES, field, op, op, scope);
            }
        }
        if (op.right instanceof FieldValue) {
        	FieldValue field = (FieldValue)op.right;
            if (op instanceof OpEqual) {
            	result = result | analyze_binary(planner, FilterOp.EQUAL, field, op.left, op, scope);
            }
            else if (op instanceof OpEqualNull) {
            	result = result | analyze_binary(planner, FilterOp.EQUALNULL, field, op.left, op, scope);
            }
            else if (op instanceof OpLarger) {
            	result = result | analyze_binary(planner, FilterOp.LESSEQUAL, field, op.left, op, scope);
            }
            else if (op instanceof OpLargerEqual) {
            	result = result | analyze_binary(planner, FilterOp.LESS, field, op.left, op, scope);
            }
            else if (op instanceof OpLess) {
            	result = result | analyze_binary(planner, FilterOp.LARGEREQUAL, field, op.left, op, scope);
            }
            else if (op instanceof OpLessEqual) {
            	result = result | analyze_binary(planner, FilterOp.LARGER, field, op.left, op, scope);
            }
        }
        return result;
    }

    private static boolean analyze_binary(
    		Planner planner, 
    		FilterOp op, 
    		FieldValue field, 
    		Operator value,
    		Operator source,
    		Node scope) {
        // the whole purpose of analysis is to translate condition to column filter. it is right here. be aware that 
        // only the filter that fits in the scope can be added. 

        PlannerField fieldLeft = (PlannerField)field.getField();
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
        
        // false means the specified condition cannot be translated to a column filter. thus we should keep it where
        // it is
        
        cf.isConstant = isConstant(node, value);
        node.addFilter(cf);
        return true;
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
                FieldValue cv = (FieldValue)it;
                if (cv.getField().owner != node) {
                    valid[0] = false;
                }
            }
        });
        return valid[0];
	}
}
