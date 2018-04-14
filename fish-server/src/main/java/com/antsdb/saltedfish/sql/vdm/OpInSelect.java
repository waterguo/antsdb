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

import java.util.List;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

public class OpInSelect extends BinaryOperator {
    CursorMaker select;
    
    public OpInSelect(Operator left, CursorMaker select) {
        super(left, null);
        this.select = select;
    }

    @Override
    public DataType getReturnType() {
        return DataType.bool();
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pValueLeft = this.left.eval(ctx, heap, params, pRecord);
        long addrResult = FishObject.allocSet(heap, select(ctx, heap, params, pRecord, pValueLeft));
        return addrResult;
    }

    boolean select(VdmContext ctx, Heap heap, Parameters params, long pMasterRecord, long pValueLeft) {
        try (Cursor cursor = this.select.make(ctx, params, pMasterRecord)) {
            for (;;) {
                long pRecord = cursor.next();
                if (pRecord == 0) {
                    break;
                }
                long pValueFromSelect = Record.get(pRecord, 0);
                if (AutoCaster.equals(heap, pValueLeft, pValueFromSelect)) {
                    return true;
                }
            }
            return false;
        }
    }

	public void explain(int level, List<ExplainRecord> records) {
        ExplainRecord rec = new ExplainRecord(-1, level, getClass().getSimpleName());
        records.add(rec);
		this.select.explain(level+1, records);
	}

	public CursorMaker getSelect() {
		return this.select;
	}

	@Override
	public String toString() {
		return "IN (...)";
	}
}