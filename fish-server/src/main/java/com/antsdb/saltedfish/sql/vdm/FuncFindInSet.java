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
import com.antsdb.saltedfish.cpp.Int4;
import com.antsdb.saltedfish.sql.DataType;

/**
 * http://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_find-in-set
 * 
 * @author wgu0
 */
public class FuncFindInSet extends Function {
	@Override
	public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pValue = this.parameters.get(0).eval(ctx, heap, params, pRecord);
        if (pValue == 0) {
        	return 0;
        }
        long pSet = this.parameters.get(1).eval(ctx, heap, params, pRecord);
        if (pSet == 0) {
        	return 0;
        }
        int result = 0;
        String value = (String)FishObject.get(heap, AutoCaster.toString(heap, pValue));
        String set = (String)FishObject.get(heap, AutoCaster.toString(heap, pSet));
        String[] array = StringUtils.split(set, ',');
        for (int i=0; i<array.length; i++) {
        	if (value.equals(array[i])) {
        		result = i+1;
        	}
        }
        return Int4.allocSet(heap, result);
	}

	@Override
	public DataType getReturnType() {
		return DataType.integer();
	}

	@Override
	public int getMinParameters() {
		return 2;
	}
}
