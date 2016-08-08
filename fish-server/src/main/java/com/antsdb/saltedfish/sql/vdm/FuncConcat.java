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

import com.antsdb.saltedfish.cpp.FishString;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

/**
 * 
 * @author wgu0
 */
public class FuncConcat extends Function {

	public FuncConcat(Operator p1, Operator p2) {
		addParameter(p1);
		addParameter(p2);
	}

	@Override
	public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
		long p1 = this.parameters.get(0).eval(ctx, heap, params, pRecord);
		long p2 = this.parameters.get(1).eval(ctx, heap, params, pRecord);
		p1 = AutoCaster.toString(heap, p1);
		p2 = AutoCaster.toString(heap, p2);
		long pResult = FishString.concat(heap, p1, p2);
		return pResult;
	}

	@Override
	public DataType getReturnType() {
		return DataType.varchar();
	}

}
