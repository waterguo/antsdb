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

import com.antsdb.saltedfish.sql.planner.SortKey;
import com.antsdb.saltedfish.util.CursorUtil;

public class Profile extends View {
    Instruction root;

    public Profile(Instruction root) {
        super(CursorUtil.toMeta(ProfileRecord.class));
        this.root = root;
        this.meta = CursorUtil.toMeta(ProfileRecord.class);
    }
    
    @Override
    public CursorMeta getCursorMeta() {
        return this.meta;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Profiler profiler = new Profiler();
        ctx.setProfiler(profiler);
        Object result = this.root.run(ctx, params, pMaster);
        if (result instanceof Cursor) {
            try (Cursor c = (Cursor)result) {
                while (c.next() != 0) {};
            }
        }
        List<ExplainRecord> explainations = new ArrayList<>();
        List<ProfileRecord> noname = new ArrayList<>();
        this.root.explain(1, explainations);
        for (ExplainRecord i:explainations) {
            ProfileRecord rec = profiler.getRecord(i.id);
            if (rec == null) {
                rec = new ProfileRecord();
            }
            rec.level = i.level;
            rec.plan = i.plan;
            rec.id = i.id;
            rec.score = i.score;
            if (rec.input_count == 0) {
                AtomicLong stats = ctx.getCursorStats(i.id);
                rec.input_count = stats.get();
            }
            noname.add(rec);
        }
        return CursorUtil.toCursor(meta, noname);
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
