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

import com.antsdb.saltedfish.sql.vdm.Operator;

/**
 * 
 * @author *-xguo0<@
 */
class Join {
    List<QueryNode> nodes = new LinkedList<>();
    Operator where;
    
    @Override
    protected Join clone() {
        Join result = new Join();
        result.nodes = new LinkedList<>();
        for (QueryNode i:nodes) {
            result.nodes.add(i.clone());
        }
        return result;
    }

    public QueryNode getQueryNode(Node owner) {
        for (QueryNode i:this.nodes) {
            if (i.node == owner) {
                return i;
            }
        }
        return null;
    }
    
}
