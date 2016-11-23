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

import com.antsdb.saltedfish.cpp.FishNumber;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

public class FuncElt extends Function {

	@Override
	public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
		long addrIndex = this.parameters.get(0).eval(ctx, heap, params, pRecord);
		if (FishNumber.isNumber(addrIndex)) {
			int index = FishNumber.intValue(addrIndex);
			if ((index >= 1) && (index < this.parameters.size())) {
				long result = this.parameters.get(index).eval(ctx, heap, params, pRecord);
				return result;
			}
		}
		return 0;
	}

	@Override
	public DataType getReturnType() {
		if (this.parameters.size() >= 2) {
			return this.parameters.get(1).getReturnType();
		}
		else {
			return null;
		}
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
