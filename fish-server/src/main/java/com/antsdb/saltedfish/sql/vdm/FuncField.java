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

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Int4;
import com.antsdb.saltedfish.sql.DataType;

public class FuncField extends Function {

	@Override
	public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
		long toSearchAddr = this.parameters.get(0).eval(ctx, heap, params, pRecord);
		if (toSearchAddr == 0) {
			return 0;
		}
		for (int i=1; i<this.parameters.size(); i++) {
			long valueAddr = this.parameters.get(i).eval(ctx, heap, params, pRecord);
			if (FishObject.equals(heap, toSearchAddr, valueAddr)) {
				return Int4.allocSet(heap, i);
			}
		}
		return 0;
	}

	@Override
	public DataType getReturnType() {
		return DataType.integer();
	}

	@Override
	public int getMinParameters() {
		return 1;
	}

	@Override
	public int getMaxParameters() {
		return -1;
	}

}
