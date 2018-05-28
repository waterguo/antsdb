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

import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;

/**
 * 
 * @author wgu0
 */
public class PlannerField extends FieldMeta {
    Node owner;
    ColumnMeta column;
    FieldMeta field;
    int index;
    
    PlannerField(Node owner, ColumnMeta column) {
        this.owner = owner;
        this.column = column;
    }
    
    PlannerField(Node owner, FieldMeta column) {
        this.owner = owner;
        this.field = column;
    }
    
    private PlannerField() {
    }

    @Override
    public String getSourceName() {
        if (this.column != null) {
            return this.column.getColumnName();
        }
        if (this.field != null) {
            return this.field.getSourceName();
        }
        return null;
    }
    
    @Override
    public String getName() {
        String result = null;
        if (this.column != null) {
            result = this.column.getColumnName();
        }
        else if (this.field != null) {
            result = this.field.getName();
        }
        return result;
    }

    @Override
    public ColumnMeta getColumn() {
        return this.column;
    }

    @Override
    public String getTableAlias() {
        return owner.alias.getTableName();
    }

    @Override
    public DataType getType() {
        DataType result = null;
        if (this.column != null) {
            result = this.column.getDataType();
        }
        else if (this.field != null) {
            result = this.field.getType();
        }
        return result;
    }

    @Override
    public String toString() {
        return this.owner.toString() + "." + getName();
    }

    public final int getIndex() {
        return this.index;
    }
 
    public void setIndex(int value) {
        this.index = value;
    }
    
    @Override
    public PlannerField clone() {
        PlannerField clone = new PlannerField();
        clone.owner = this.owner;
        clone.column = this.column;
        clone.field = this.field;
        clone.index = this.index;
        return clone;
    }
}
