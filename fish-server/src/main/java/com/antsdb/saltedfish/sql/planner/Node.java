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

import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.PrimaryKeyMeta;
import com.antsdb.saltedfish.sql.meta.RuleMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.Operator;

/**
 * Node is a participant in a query that can product a cursor
 *  
 * @author wgu0
 */
public class Node {
    boolean freeze = false;
    TableMeta table;
    CursorMaker maker;
    ObjectName alias;
    List<PlannerField> fields = new ArrayList<>();
    boolean isOuter;
    boolean isParent = false;
    Operator joinCondition;
    LinkedList<RowSet> union = new LinkedList<>();
    public boolean isUsed = false;
    Planner planner;
    
    // below are fields related to union
    Operator where;
    
    PlannerField findField(FieldMeta field) {
        for (PlannerField i:fields) {
            if (i == field) {
                return i;
            }
        }
        return null;
    }

    int findFieldPos(PlannerField field) {
        if (field.column != null) {
            return field.column.getColumnId() + 1;
        }
        else {
            for (int i=0; i<this.fields.size(); i++) {
                if (this.fields.get(i) == field) {
                    return i;
                }
            }
            return -1;
        }
    }

    @Override
    public String toString() {
        return this.alias.toString();
    }

    public boolean isUnique(List<PlannerField> key) {
        if (this.table == null) {
            return false;
        }
        if (isUnique(this.table.getPrimaryKey(), key)) {
            return true;
        }
        for (RuleMeta<?> i:table.getIndexes()) {
            if (isUnique(i, key)) {
                return true;
            }
        }
        return false;
    }

    private boolean isUnique(RuleMeta<?> rule, List<PlannerField> key) {
        if (!(rule instanceof PrimaryKeyMeta)) {
            if (!(rule instanceof IndexMeta)) {
                return false;
            }
            IndexMeta index = (IndexMeta)rule;
            if (!index.isUnique()) {
                return false;
            }
        }
        int[] ruleColumns = rule.getRuleColumns();
        if (key.size() < ruleColumns.length) {
            return false;
        }
        for (int i=0; i<ruleColumns.length; i++) {
            ColumnMeta column = key.get(i).column;
            if (column == null) {
                return false;
            }
            if (ruleColumns[i] != column.getId()) {
                return false;
            }
        }
        return true;
    }
    
    boolean isCursor() {
        return this.table == null;
    }
    
    public List<RowSet> getUnion() {
        return this.union;
    }
    
    CursorMaker getCursorMaker(int parentWidth) {
        return (this.maker != null) ? this.maker : this.planner.run(parentWidth);
    }
    
    /**
     * width, in most of cases is number of fields from the node. but if the node is a table, width is
     * the max column_id + 2 because we need to consider deleted columns.  
     *  
     * @return
     */
    public int getWidth() {
        if (this.table != null) {
            return this.table.getMaxColumnId() + 2;
        }
        else {
            return this.fields.size();
        }
    }
}
