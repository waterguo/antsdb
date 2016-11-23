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
import java.util.Locale;

import org.apache.commons.lang.NotImplementedException;

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
			if (specifier == 'a') {
				throw new NotImplementedException();
			}
			else if (specifier == 'b') {
				throw new NotImplementedException();
			}
			else if (specifier == 'c') {
				buf.append(calendar.get(Calendar.MONTH + 1));
			}
			else if (specifier == 'd') {
				int day = calendar.get(Calendar.DAY_OF_MONTH);
				if (day < 10) {
					buf.append('0');
				}
				buf.append(day);
			}
			else if (specifier == 'D') {
				throw new NotImplementedException();
			}
			else if (specifier == 'e') {
				buf.append(calendar.get(Calendar.DAY_OF_MONTH));
			}
			else if (specifier == 'f') {
				buf.append(calendar.get(Calendar.MILLISECOND * 1000));
			}
			else if (specifier == 'H') {
				buf.append(calendar.get(Calendar.HOUR));
			}
			else if (specifier == 'h') {
				buf.append(calendar.get(Calendar.HOUR) % 13);
			}
			else if (specifier == 'i') {
				buf.append(calendar.get(Calendar.MINUTE));
			}
			else if (specifier == 'I') {
				buf.append(calendar.get(Calendar.HOUR) % 13);
			}
			else if (specifier == 'j') {
				buf.append(calendar.get(Calendar.DAY_OF_YEAR));
			}
			else if (specifier == 'k') {
				buf.append(calendar.get(Calendar.HOUR));
			}
			else if (specifier == 'l') {
				buf.append(calendar.get(Calendar.HOUR) % 13);
			}
			else if (specifier == 'm') {
				int month = calendar.get(Calendar.MONTH) + 1;
				if (month < 10) {
					buf.append('0');
				}
				buf.append(calendar.get(Calendar.MONTH) + 1);
			}
			else if (specifier == 'M') {
				buf.append(calendar.getDisplayName(Calendar.MONTH, Calendar.LONG, Locale.getDefault()));
			}
			else if (specifier == 'p') {
				int hour = calendar.get(Calendar.HOUR);
				buf.append(hour < 12 ? "AM" : "PM");
			}
			else if (specifier == 'r') {
				int hour = calendar.get(Calendar.HOUR);
				hour = hour % 13;
				if (hour < 10) {
					buf.append('0');
				}
				buf.append(hour);
				buf.append(':');
				int minute = calendar.get(Calendar.MINUTE);
				if (minute < 10) {
					buf.append('0');
				}
				buf.append(minute);
				buf.append(':');
				int second = calendar.get(Calendar.SECOND);
				if (second < 10) {
					buf.append('0');
				}
				buf.append(second);
				buf.append(hour < 12 ? " AM" : " PM");
			}
			else if (specifier == 's') {
				buf.append(calendar.get(Calendar.SECOND));
			}
			else if (specifier == 'S') {
				buf.append(calendar.get(Calendar.SECOND));
			}
			else if (specifier == 'T') {
				throw new NotImplementedException();
			}
			else if (specifier == 'u') {
				buf.append(calendar.get(Calendar.WEEK_OF_YEAR));
			}
			else if (specifier == 'U') {
				throw new NotImplementedException();
			}
			else if (specifier == 'v') {
				throw new NotImplementedException();
			}
			else if (specifier == 'V') {
				throw new NotImplementedException();
			}
			else if (specifier == 'w') {
				throw new NotImplementedException();
			}
			else if (specifier == 'W') {
				throw new NotImplementedException();
			}
			else if (specifier == 'x') {
				throw new NotImplementedException();
			}
			else if (specifier == 'X') {
				throw new NotImplementedException();
			}
			else if (specifier == 'y') {
				buf.append(calendar.get(Calendar.YEAR) % 100);
			}
			else if (specifier == 'Y') {
				buf.append(calendar.get(Calendar.YEAR));
			}
			else if (specifier == '%') {
				buf.append('%');
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

	@Override
	public int getMinParameters() {
		return 2;
	}

}
