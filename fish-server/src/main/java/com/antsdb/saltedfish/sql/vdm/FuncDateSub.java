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

import com.antsdb.saltedfish.cpp.FishTimestamp;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.sql.DataType;

/**
 * 
 * @author *-xguo0<@
 */
public class FuncDateSub extends Function {
	static final long MILLIS_IN_A_DAY = 24 * 60 * 60 * 1000l;
	
	@Override
	public int getMinParameters() {
		return 2;
	}

	@Override
	public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pX = this.parameters.get(0).eval(ctx, heap, params, pRecord);
        long pY = this.parameters.get(1).eval(ctx, heap, params, pRecord);
        if (Value.getType(heap, pY) == Value.TYPE_NUMBER) {
        	// default is days without unit of time measurement suffix
        	long y = AutoCaster.getLong(pY);
        	pY = FishTimestamp.allocSet(heap, y * MILLIS_IN_A_DAY);
        }
        pY = AutoCaster.negate(heap, pY);
        return AutoCaster.addTime(heap, pX, pY);
	}

	@Override
	public DataType getReturnType() {
        return DataType.timestamp();
	}
}
