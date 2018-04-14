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
import java.util.LinkedList;
import java.util.List;

import com.antsdb.saltedfish.sql.meta.RuleMeta;
import com.antsdb.saltedfish.sql.vdm.Operator;

/**
 * 
 * @author *-xguo0<@
 */
class QueryNode {
    Node node;
    List<ColumnFilter> filters;
    Operator condition;

    @Override
    protected QueryNode clone() {
        QueryNode result = new QueryNode();
        result.node = this.node;
        if (this.filters != null) {
            result.filters = new LinkedList<>(this.filters);
        }
        return result;
    }

    public void addFilter(ColumnFilter cf) {
        if (this.filters == null) {
            this.filters = new ArrayList<>();
        }
        this.filters.add(cf);
    }

    public boolean isOuter() {
        return this.node.isOuter;
    }

    public boolean isParent() {
        return this.node.isParent;
    }

    @Override
    public String toString() {
        return this.node.toString();
    }

    public List<RuleMeta<?>> getKeys() {
        List<RuleMeta<?>> result = new ArrayList<>();
        result.add(this.node.table.getPrimaryKey());
        result.addAll(this.node.table.getIndexes());
        return result;
    }
}
