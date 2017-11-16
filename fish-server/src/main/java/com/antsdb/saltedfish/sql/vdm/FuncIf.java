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
import com.antsdb.saltedfish.sql.DataType;

public class FuncIf extends Function {
	
	@Override
	public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
		long pTest = this.parameters.get(0).eval(ctx, heap, params, pRecord);
		Boolean test = (Boolean)FishObject.get(heap, pTest);
		long pResult;
		if ((test != null) && test) {
		    pResult = this.parameters.get(1).eval(ctx, heap, params, pRecord);
		}
		else {
            pResult = this.parameters.get(2).eval(ctx, heap, params, pRecord);
		}
		return pResult;
	}

	@Override
	public DataType getReturnType() {
		return this.parameters.get(1).getReturnType();
	}

	@Override
	public int getMinParameters() {
		return 3;
	}

}
