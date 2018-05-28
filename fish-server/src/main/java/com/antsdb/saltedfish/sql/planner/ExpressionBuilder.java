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
import java.util.Collection;
import java.util.List;

import com.antsdb.saltedfish.nosql.SlowRow;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.vdm.FieldValue;
import com.antsdb.saltedfish.sql.vdm.FuncNow;
import com.antsdb.saltedfish.sql.vdm.LongValue;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.OpAnd;
import com.antsdb.saltedfish.sql.vdm.OpEqual;
import com.antsdb.saltedfish.sql.vdm.OpOr;
import com.antsdb.saltedfish.sql.vdm.Operator;

/**
 * 
 * @author *-xguo0<@
 */
class ExpressionBuilder {
    List<Node> nodes = new ArrayList<>();
    
    ExpressionBuilder(Collection<Node> nodes) {
        this.nodes.addAll(nodes);
    }
    
    ColumnMeta buildColumn(String name) {
        ColumnMeta result = new ColumnMeta(null, new SlowRow(1));
        result.setColumnName(name);
        return result;
    }
    
    PlannerField buildField(Node node, String column) {
        PlannerField result = new PlannerField(node, buildColumn(column));
        return result;
    }
    
    Node buildNode(String name) {
        Node result = new Node();
        result.alias = new ObjectName(null, name);
        result.fields.add(buildField(result, name + "_id"));
        result.fields.add(buildField(result, name + "_name"));
        result.fields.add(buildField(result, name + "_city"));
        this.nodes.add(result);
        return result;
    }

    Operator func() {
        return new FuncNow();
    }
    
    Operator number(long value) {
        return new LongValue(value);
    }
    
    Operator field(String table, String column) {
        for (Node i:nodes) {
            if (!i.alias.table.equals(table)) {
                continue;
            }
            for (PlannerField j:i.fields) {
                if (j.getName().equals(column)) {
                    return new FieldValue(j);
                }
            }
        }
        return null;
    }
    
    Operator eq(Operator x, Operator y) {
        return new OpEqual(x, y);
    }
    
    Operator and(Operator x, Operator y) {
        return new OpAnd(x, y);
    }
    
    Operator or(Operator x, Operator y) {
        return new OpOr(x, y);
    }
}
