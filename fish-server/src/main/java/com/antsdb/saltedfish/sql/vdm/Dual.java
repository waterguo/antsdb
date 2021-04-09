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

import java.util.ArrayList;
import java.util.List;

import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.planner.SortKey;
import com.antsdb.saltedfish.util.CursorUtil;

public class Dual extends CursorMaker {
    static List<Record> _list = new ArrayList<Record>();
    static CursorMeta _meta = new CursorMeta();
    
    static {
        Record rec = new HashMapRecord();
        rec.set(0, "X");
        _list.add(rec);
        _meta.addColumn(new FieldMeta("DUMMY", DataType.varchar()));
    }
    
    @Override
    public CursorMeta getCursorMeta() {
        return _meta;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Cursor c = CursorUtil.toCursor(_meta, _list);
        return c;
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
