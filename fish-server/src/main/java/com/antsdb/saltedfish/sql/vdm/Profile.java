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
import java.util.concurrent.atomic.AtomicLong;

import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.planner.SortKey;
import com.antsdb.saltedfish.util.CursorUtil;

public class Profile extends CursorMaker {
    Instruction root;
    CursorMeta meta = new CursorMeta();

    public Profile(Instruction root) {
        super();
        this.root = root;
        this.meta.addColumn(new FieldMeta("id", DataType.integer()));
        this.meta.addColumn(new FieldMeta("level", DataType.integer()));
        this.meta.addColumn(new FieldMeta("plan", DataType.varchar()));
        this.meta.addColumn(new FieldMeta("stats", DataType.varchar()));
    }
    
    @Override
    public CursorMeta getCursorMeta() {
        return this.meta;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Object result = this.root.run(ctx, params, pMaster);
        if (result instanceof Cursor) {
            try (Cursor c = (Cursor)result) {
                while (c.next() != 0) {};
            }
        }
        List<ProfileRecord> records = new ArrayList<ProfileRecord>();
        List<ExplainRecord> explainations = new ArrayList<ExplainRecord>();
        this.root.explain(1, explainations);
        for (ExplainRecord i:explainations) {
            ProfileRecord rec = new ProfileRecord();
            rec.setLevel(i.level)
               .setPlan(i.plan)
               .setMakerId(i.id);
            AtomicLong stats = ctx.getCursorStats(i.id);
            rec.setStats(Long.toString(stats.get()));
            records.add(rec);
        }
        return CursorUtil.toCursor(meta, records);
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return false;
    }

}
