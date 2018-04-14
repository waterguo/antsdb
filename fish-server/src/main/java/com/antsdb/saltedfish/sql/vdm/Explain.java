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

import java.util.ArrayList;
import java.util.List;

import com.antsdb.saltedfish.sql.planner.SortKey;
import com.antsdb.saltedfish.util.CursorUtil;

public class Explain extends ViewMaker {
    Instruction root;
    
    public Explain(Instruction root) {
        super(CursorUtil.toMeta(ExplainRecord.class));
        this.root = root;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        List<ExplainRecord> records = new ArrayList<ExplainRecord>();
        this.root.explain(1, records);
        return CursorUtil.toCursor(meta, records);
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return false;
    }
}    
