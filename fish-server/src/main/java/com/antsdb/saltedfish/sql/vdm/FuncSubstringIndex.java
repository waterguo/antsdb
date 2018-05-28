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

import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Unicode16;
import com.antsdb.saltedfish.sql.DataType;

/**
 * @see http://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substring-index
 * 
 * @author *-xguo0<@
 */
public class FuncSubstringIndex extends Function {
	@Override
	public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pString = this.parameters.get(0).eval(ctx, heap, params, pRecord);
        if (pString == 0) {
        	return 0;
        }
        long pDelimiter = this.parameters.get(1).eval(ctx, heap, params, pRecord);
        if (pDelimiter == 0) {
        	return 0;
        }
        long pCount = this.parameters.get(2).eval(ctx, heap, params, pRecord);
        if (pCount == 0) {
        	return 0;
        }
        String str = (String)FishObject.get(heap, AutoCaster.toString(heap, pString));
        String delim = (String)FishObject.get(heap, AutoCaster.toString(heap, pDelimiter));
        Long count = (Long)FishObject.get(heap, AutoCaster.toNumber(heap, pCount));
        String result = null;
        if (count > 0) {
        	result = indexOf(str, delim, count);
        }
        else if (count < 0) {
        	result = lastIndexOf(str, delim, -count);
        }
        return Unicode16.allocSet(heap, result);
	}

	private String lastIndexOf(String str, String delim, Long count) {
        int pos = str.length();
        for (int i=0; i<count; i++) {
        	pos = StringUtils.lastIndexOf(str, delim, pos-1);
        	if (pos < 0) {
        		return str;
        	}
        }
        return str.substring(pos+1);
	}

	private String indexOf(String str, String delim, Long count) {
        int pos = -1;
        for (int i=0; i<count; i++) {
        	pos = StringUtils.indexOf(str, delim, pos+1);
        	if (pos < 0) {
        		return str;
        	}
        }
        return str.substring(0, pos);
	}

	@Override
	public DataType getReturnType() {
		return DataType.varchar();
	}

	@Override
	public int getMinParameters() {
		return 3;
	}
}
