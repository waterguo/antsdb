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

/**
 * 
 * @author *-xguo0<@
 */
public class FuncTrim extends Function {

	@Override
	public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        String str = (String)Util.eval(ctx, this.parameters.get(0), params, pRecord);
        if (str == null) {
        	return 0;
        }
        str = str.trim();
        long addr = FishObject.allocSet(heap, str);
        return addr;
	}

	@Override
	public DataType getReturnType() {
        return DataType.varchar();
	}

	@Override
	public int getMinParameters() {
		return 1;
	}
}
