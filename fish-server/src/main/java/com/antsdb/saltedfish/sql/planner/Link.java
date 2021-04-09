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
import java.util.List;

import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;
import com.antsdb.saltedfish.sql.vdm.Operator;

/**
 * Link is a logic step that walks from one Node to another
 *  
 * @author wgu0
 */
class Link {
    Node to;
    CursorMaker maker;
    Link previous;
    List<ColumnFilter> consumed = new ArrayList<>();
    Operator join;
    boolean isUnion = false;
    /** position in the where clause that results in this link */
    int pos;
    int width;
    
    Link(Node to) {
        this.to = to;
    }
    
    boolean exists(Node node) {
        if (node == this.to) {
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

    PlannerField findField(FieldMeta field) {
        PlannerField result = this.to.findField(field);
        if (result != null) {
            return result;
        }
        if (this.previous != null) {
            result = this.previous.findField(field);
        }
        return result;
    }

    @Override
    public String toString() {
        return this.to.toString();
    }
    
    int getLevels() {
        if (this.previous != null) {
            return this.previous.getLevels() + 1;
        }
        return 1;
    }

    float getScore() {
        return maker.getScore();
    }

    public boolean isUnique(List<PlannerField> key) {
        if (key == null) return false;
        if (this.to.isOuter) return false;
        if (this.previous != null) {
            return this.previous.isUnique(key); 
        }
        return this.to.isUnique(key);
    }

    public Link getRoot() {
        return (this.previous == null) ? this : this.previous.getRoot();
    }
    
    public int getWidth() {
        int result = this.previous != null ? this.previous.getWidth() : 0;
        result += this.width != 0 ? this.width : this.to.getWidth();
        return result;
    }
}
