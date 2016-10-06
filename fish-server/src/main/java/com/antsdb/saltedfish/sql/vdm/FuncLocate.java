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

import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

/**
 * @see http://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
 * @author *-xguo0<@
 */
public class FuncLocate extends Function {
	@Override
	public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pString = this.parameters.get(1).eval(ctx, heap, params, pRecord);
        if (pString == 0) {
        	return 0;
        }
        long pSubstr = this.parameters.get(0).eval(ctx, heap, params, pRecord);
        if (pSubstr == 0) {
        	return 0;
        }
        String str = (String)FishObject.get(heap, AutoCaster.toString(heap, pString));
        String substr = (String)FishObject.get(heap, AutoCaster.toString(heap, pSubstr));
        int pos = StringUtils.indexOf(str, substr)+1;
        return FishObject.allocSet(heap, pos);
	}

	@Override
	public DataType getReturnType() {
		return DataType.integer();
	}
}
