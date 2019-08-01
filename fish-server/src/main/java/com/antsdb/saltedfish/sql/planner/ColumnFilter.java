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

import com.antsdb.saltedfish.sql.vdm.Operator;
import com.antsdb.saltedfish.sql.vdm.StringLiteral;

/**
 * conditions that operates on a single column.
 * 
 * @author wgu0
 */
class ColumnFilter {
    PlannerField field;
    FilterOp op;
    Object operand;
    boolean isConstant;
    Operator source;
    int version;

    public ColumnFilter(PlannerField field, FilterOp op, Object operand, Operator source) {
        super();
        this.field = field;
        this.op = op;
        this.operand = operand;
        this.source = source;
    }

    public boolean isRangeScan() {
        switch (this.op) {
            case LESS:
            case LESSEQUAL:
            case LARGER:
            case LARGEREQUAL:
                return true;
            case LIKE: {
                if (operand instanceof StringLiteral) {
                    String value = ((StringLiteral) operand).getValue();
                    return !value.startsWith("%");
                }
            }
            default:
        }
        return false;
    }

    private String getOpString() {
        switch (this.op) {
        case LESS:
            return "<";
        case LESSEQUAL:
            return "<=";
        case LARGER:
            return ">";
        case LARGEREQUAL:
            return ">=";
        case EQUAL:
            return "=";
        case EQUALNULL:
            return "==";
        default:
            return " " + this.op.toString() + " ";
        }
    }
    
    @Override
    public String toString() {
        return this.field + getOpString() + this.operand.toString();
    }
    
}
