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

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Locale;

import org.apache.commons.lang.NotImplementedException;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.FishTimestamp;
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
        Timestamp time = FishTimestamp.isAllZero(pDate) ? null : (Timestamp)FishObject.get(heap, pDate);
        long pFormat = this.parameters.get(1).eval(ctx, heap, params, pRecord);
        String format = AutoCaster.getString(heap, pFormat);
		String result = format(format, time);
		return FishObject.allocSet(heap, result);
	}

	static String format(String format, Timestamp time) {
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
			Calendar calendar = (time == null) ? null : Calendar.getInstance();
			if (time != null) {
			    calendar.setTimeInMillis(time.getTime());
			}
			if (specifier == 'a') {
				throw new NotImplementedException();
			}
			else if (specifier == 'b') {
				throw new NotImplementedException();
			}
			else if (specifier == 'c') {
			    if (calendar != null) {
			        buf.append(calendar.get(Calendar.MONTH) + 1);
			    }
			    else {
			        buf.append("0");
			    }
			}
			else if (specifier == 'd') {
                if (calendar != null) {
                    int day = calendar.get(Calendar.DAY_OF_MONTH);
                    if (day < 10) {
                        buf.append('0');
                    }
                    buf.append(day);
                }
                else {
                    buf.append("00");
                }
			}
			else if (specifier == 'D') {
                if (calendar != null) {
                    int day = calendar.get(Calendar.DAY_OF_MONTH);
                    buf.append(day);
                    switch (day) {
                    case 1: buf.append("st");
                    case 2: buf.append("nd");
                    case 3: buf.append("rd");
                    default: buf.append("th");
                    }
                }
                else {
                    buf.append("00");
                }
			}
			else if (specifier == 'e') {
                if (calendar != null) {
                    buf.append(calendar.get(Calendar.DAY_OF_MONTH));
                }
                else {
                    buf.append("0");
                }
			}
			else if (specifier == 'f') {
                if (calendar != null) {
                    buf.append(calendar.get(Calendar.MILLISECOND * 1000));
                }
                else {
                    buf.append("0");
                }
			}
			else if (specifier == 'H') {
                if (calendar != null) {
                    buf.append(calendar.get(Calendar.HOUR));
                }
                else {
                    buf.append("00");
                }
			}
			else if (specifier == 'h') {
                if (calendar != null) {
                    buf.append(calendar.get(Calendar.HOUR) % 13);
                }
                else {
                    buf.append("0");
                }
			}
			else if (specifier == 'i') {
                if (calendar != null) {
                    buf.append(calendar.get(Calendar.MINUTE));
                }
                else {
                    buf.append("00");
                }
			}
			else if (specifier == 'I') {
                if (calendar != null) {
                    buf.append(calendar.get(Calendar.HOUR) % 13);
                }
                else {
                    buf.append("0");
                }
			}
			else if (specifier == 'j') {
                if (calendar != null) {
                    buf.append(calendar.get(Calendar.DAY_OF_YEAR));
                }
                else {
                    buf.append("0");
                }
			}
			else if (specifier == 'k') {
                if (calendar != null) {
                    buf.append(calendar.get(Calendar.HOUR));
                }
                else {
                    buf.append("0");
                }
			}
			else if (specifier == 'l') {
                if (calendar != null) {
                    buf.append(calendar.get(Calendar.HOUR) % 13);
                }
                else {
                    buf.append("0");
                }
			}
			else if (specifier == 'm') {
                if (calendar != null) {
                    int month = calendar.get(Calendar.MONTH) + 1;
                    if (month < 10) {
                        buf.append('0');
                    }
                    buf.append(calendar.get(Calendar.MONTH) + 1);
                }
                else {
                    buf.append("00");
                }
			}
			else if (specifier == 'M') {
                if (calendar != null) {
                    buf.append(calendar.getDisplayName(Calendar.MONTH, Calendar.LONG, Locale.getDefault()));
                }
                else {
                    buf.append("");
                }
			}
			else if (specifier == 'p') {
                if (calendar != null) {
                    int hour = calendar.get(Calendar.HOUR);
                    buf.append(hour < 12 ? "AM" : "PM");
                }
                else {
                    buf.append("AM");
                }
			}
			else if (specifier == 'r') {
                if (calendar != null) {
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
                else {
                    buf.append("00 AM");
                }
			}
			else if (specifier == 's') {
                if (calendar != null) {
                    buf.append(calendar.get(Calendar.SECOND));
                }
                else {
                    buf.append("0");
                }
			}
			else if (specifier == 'S') {
                if (calendar != null) {
                    buf.append(calendar.get(Calendar.SECOND));
                }
                else {
                    buf.append("0");
                }
			}
			else if (specifier == 'T') {
				throw new NotImplementedException();
			}
			else if (specifier == 'u') {
                if (calendar != null) {
                    buf.append(calendar.get(Calendar.WEEK_OF_YEAR));
                }
                else {
                    buf.append("0");
                }
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
                if (calendar != null) {
                    buf.append(calendar.get(Calendar.YEAR));
                }
                else {
                    buf.append("0000");
                }
			}
			else if (specifier == 'X') {
				throw new NotImplementedException();
			}
			else if (specifier == 'y') {
                if (calendar != null) {
                    buf.append(calendar.get(Calendar.YEAR) % 100);
                }
                else {
                    buf.append("00");
                }
			}
			else if (specifier == 'Y') {
                if (calendar != null) {
                    buf.append(calendar.get(Calendar.YEAR));
                }
                else {
                    buf.append("0000");
                }
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
