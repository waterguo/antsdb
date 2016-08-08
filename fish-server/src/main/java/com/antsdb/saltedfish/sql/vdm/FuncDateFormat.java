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

import java.sql.Timestamp;
import java.util.Calendar;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

/**
 * https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_date-format
 *  
 * @author wgu0
 */
public class FuncDateFormat extends Function {

	@Override
	public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pDate = this.parameters.get(0).eval(ctx, heap, params, pRecord);
        if (pDate == 0) {
        	return 0;
        }
        pDate = AutoCaster.toTimestamp(heap, pDate);
        Timestamp time = (Timestamp)FishObject.get(heap, pDate);
        long pFormat = this.parameters.get(1).eval(ctx, heap, params, pRecord);
        String format = (String)FishObject.get(heap, pFormat);
		String result = format(format, time);
		return FishObject.allocSet(heap, result);
	}

	private String format(String format, Timestamp time) {
		StringBuilder buf = new StringBuilder();
		for (int i=0; i<format.length(); i++) {
			char ch = format.charAt(i);
			if (ch != '%') {
				buf.append(ch);
				continue;
			}
			if (i >= (format.length()-1)) {
				buf.append(ch);
				continue;
			}
			char specifier = format.charAt(++i);
			Calendar calendar = Calendar.getInstance();
			calendar.setTimeInMillis(time.getTime());
			if (specifier == 'y') {
				buf.append(calendar.get(Calendar.YEAR) % 100);
			}
			else if (specifier == 'u') {
				buf.append(calendar.get(Calendar.WEEK_OF_YEAR));
			}
			else {
				buf.append(specifier);
			}
		}
		return buf.toString();
	}

	@Override
	public DataType getReturnType() {
		return DataType.varchar();
	}

}
