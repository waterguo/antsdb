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
package com.antsdb.saltedfish.sql.vdm;

import java.util.List;

import com.antsdb.saltedfish.sql.planner.SortKey;

/**
 * 
 * @author *-xguo0<@
 */
public class View extends CursorMaker {
    protected ObjectName name;
    protected CursorMeta meta;
    
    public View(CursorMeta meta) {
        this.meta = meta;
    }
    
    public ObjectName getName() {
        return this.name;
    }

    public void setName(ObjectName name) {
        this.name = name;
        for (FieldMeta i:this.meta.fields) {
            i.setSourceTable(name);
        }
    }
    
    @Override
    public CursorMeta getCursorMeta() {
        return this.meta;
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return false;
    }

    public Object run(VdmContext ctx, Parameters params) {
        return null;
    }

    @Override
    public float getScore() {
        return 0;
    }
}
