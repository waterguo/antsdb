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

import com.antsdb.saltedfish.nosql.Statistician;
import com.antsdb.saltedfish.nosql.TableStats;
import com.antsdb.saltedfish.sql.meta.TableMeta;

/**
 * 
 * @author *-xguo0<@
 */
abstract class Path {
    QueryNode node;
    Path previous;
    
    abstract double getScore(Statistician stats);

    Path(QueryNode node) {
        this.node = node;
    }
    
    Path getRoot() {
        return (this.previous != null) ? this.previous.getRoot() : this;
    }
    
    protected final long getRowCount(Statistician stats) {
        TableMeta table = this.node.node.table;
        if (table == null) {
            // prolly a view
            return 1000;
        }
        TableStats tableStats = stats.getStats().get(table.getHtableId());
        if (tableStats == null) {
            return 1000;
        }
        long result = tableStats.count;
        if (result < 0) {
            // lost count
            return 1000;
        }
        return result;
    }
    
    boolean exists(QueryNode node) {
        if (node == this.node) {
            return true;
        }
        else {
            if (this.previous != null) {
                return this.previous.exists(node);
            }
            else {
                return false;
            }
        }
    }

    @Override
    public String toString() {
        String name = getClass().getSimpleName() + "(" + this.node.toString() + ")";
        return (this.previous != null) ? this.previous.toString() + " > " + name : name;
    }
}
