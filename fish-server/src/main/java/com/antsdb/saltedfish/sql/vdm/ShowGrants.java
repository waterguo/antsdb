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
package com.antsdb.saltedfish.sql.vdm;

import java.util.Collections;
import java.util.List;

import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.planner.SortKey;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class ShowGrants extends CursorMaker {
    CursorMeta meta = new CursorMeta();

    public ShowGrants() {
        meta.addColumn(new FieldMeta("Grants", DataType.varchar()));
    }

    @Override
    public CursorMeta getCursorMeta() {
        return this.meta;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Cursor c = CursorUtil.toCursor(meta, Collections.emptyList());
        return c;
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return false;
    }
}
