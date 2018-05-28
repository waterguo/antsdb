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

import com.antsdb.saltedfish.cpp.FishBool;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

/**
 * IS [NOT] NULL operator
 * 
 * @author wgu0
 */
public class OpIsNull extends UnaryOperator {
	
	public OpIsNull(Operator upstream) {
		super(upstream);
	}

	@Override
	public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pResult = this.upstream.eval(ctx, heap, params, pRecord);
        boolean result = pResult == 0;
        return FishBool.allocSet(heap, result);
	}

	@Override
	public DataType getReturnType() {
		return DataType.bool();
	}

	@Override
	public String toString() {
		return this.upstream.toString() + " IS NULL";
	}

	
}
