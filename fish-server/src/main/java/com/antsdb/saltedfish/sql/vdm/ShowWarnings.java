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

import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.planner.SortKey;

/**
 * 
 * @author wgu0
 */
public class ShowWarnings extends CursorMaker {
    CursorMeta meta = new CursorMeta();
    
    public ShowWarnings() {
        meta.addColumn(new FieldMeta("Level", DataType.varchar()))
            .addColumn(new FieldMeta("Code", DataType.integer()))
            .addColumn(new FieldMeta("Message", DataType.longtype()));
    }
    
    @Override
    public CursorMeta getCursorMeta() {
        return this.meta;
    }

    @Override
    public Cursor make(VdmContext ctx, Parameters params, long pMaster) {
        return new EmptyCursor(meta);
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return false;
    }

    @Override
    public float getScore() {
        return 0;
    }
}
