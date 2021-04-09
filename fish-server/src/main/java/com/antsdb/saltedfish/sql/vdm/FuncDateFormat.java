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
import org.apache.commons.lang.StringUtils;

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
        if (pDate == 0) {
            return 0;
        }
        Timestamp time = (Timestamp)FishObject.get(heap, pDate);
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
            if (FishTimestamp.isAllZero(time)) {
                formatZeroTimestamp(buf, specifier);
            }
            else {
                Calendar calendar = Calendar.getInstance();
                calendar.setTimeInMillis(time.getTime());
                format(buf, specifier, calendar);
            }
        }
        return buf.toString();
    }

    private static void formatZeroTimestamp(StringBuilder buf, char specifier) {
        if (specifier == 'd') {
            buf.append("00");
        }
        else if (specifier == 'D') {
            buf.append("00");
        }
        else if (specifier == 'f') {
            buf.append("000000");
        }
        else if (specifier == 'H') {
            buf.append("00");
        }
        else if (specifier == 'h' || specifier == 'I') {
            buf.append("00");
        }
        else if (specifier == 'i') {
            buf.append("00");
        }
        else if (specifier == 'j') {
            buf.append("00");
        }
        else if (specifier == 'k') {
            buf.append("00");
        }
        else if (specifier == 'm') {
            buf.append("00");
        }
        else if (specifier == 'p') {
            buf.append("AM");
        }
        else if (specifier == 'r') {
            buf.append("00 AM");
        }
        else if (specifier == 's' || specifier == 'S') {
            buf.append("00");
        }
        else if (specifier == 'T') {
            buf.append("00 AM");
        }
        else if (specifier == 'u') {
            buf.append("00");
        }
        else if (specifier == 'U') {
            buf.append("00");
        }
        else if (specifier == 'x') {
            buf.append("0000");
        }
        else if (specifier == 'y') {
            buf.append("00");
        }
        else if (specifier == 'Y') {
            buf.append("0000");
        }
        else {
            buf.append("0");
        }
    }

    private static void format(StringBuilder buf, char specifier, Calendar calendar) {
        if (specifier == 'a') {
            String value = calendar.getDisplayName(Calendar.DAY_OF_WEEK, Calendar.SHORT, Locale.getDefault());
            buf.append(value);
        }
        else if (specifier == 'b') {
            throw new NotImplementedException();
        }
        else if (specifier == 'c') {
            int value = calendar.get(Calendar.MONTH) + 1;
            buf.append(String.valueOf(value));
        }
        else if (specifier == 'd') {
            int value = calendar.get(Calendar.DAY_OF_MONTH);
            buf.append(StringUtils.leftPad(String.valueOf(value), 2, '0'));
        }
        else if (specifier == 'D') {
            int day = calendar.get(Calendar.DAY_OF_MONTH);
            buf.append(day);
            switch (day) {
            case 1: buf.append("st");
            case 2: buf.append("nd");
            case 3: buf.append("rd");
            default: buf.append("th");
            }
        }
        else if (specifier == 'e') {
            int value = calendar.get(Calendar.DAY_OF_MONTH);
            buf.append(String.valueOf(value));
        }
        else if (specifier == 'f') {
            int value = calendar.get(Calendar.MILLISECOND);
            buf.append(StringUtils.leftPad(String.valueOf(value), 6, '0'));
        }
        else if (specifier == 'H') {
            int value = calendar.get(Calendar.HOUR_OF_DAY);
            buf.append(StringUtils.leftPad(String.valueOf(value), 2, '0'));
        }
        else if (specifier == 'h' || specifier == 'I') {
            int value = calendar.get(Calendar.HOUR) % 12;
            if (calendar != null && value == 0) value = 12;
            buf.append(StringUtils.leftPad(String.valueOf(value), 2, '0'));
        }
        else if (specifier == 'i') {
            int value = calendar.get(Calendar.MINUTE);
            buf.append(StringUtils.leftPad(String.valueOf(value), 2, '0'));
        }
        else if (specifier == 'j') {
            int day = calendar.get(Calendar.DAY_OF_YEAR);
            buf.append(StringUtils.leftPad(String.valueOf(day), 3, '0'));
        }
        else if (specifier == 'k') {
            int hour = calendar.get(Calendar.HOUR_OF_DAY);
            buf.append(StringUtils.leftPad(String.valueOf(hour), 2, '0'));
        }
        else if (specifier == 'l') {
            int value = calendar.get(Calendar.HOUR) % 12;
            if (calendar != null && value == 0) value = 12;
            buf.append(String.valueOf(value));
        }
        else if (specifier == 'm') {
            int month = calendar.get(Calendar.MONTH) + 1;
            buf.append(StringUtils.leftPad(String.valueOf(month), 2, '0'));
        }
        else if (specifier == 'M') {
            String value = calendar.getDisplayName(Calendar.MONTH, Calendar.LONG, Locale.getDefault());
            buf.append(value);
        }
        else if (specifier == 'p') {
            int hour = calendar.get(Calendar.AM_PM);
            buf.append(hour < 1  ? "AM" : "PM");
        }
        else if (specifier == 'r') {
            format(buf, 'h', calendar);
            buf.append(':');
            format(buf, 'i', calendar);
            buf.append(':');
            format(buf, 's', calendar);
            buf.append(' ');
            format(buf, 'p', calendar);
        }
        else if (specifier == 's' || specifier == 'S') {
            int second = calendar.get(Calendar.SECOND);
            buf.append(StringUtils.leftPad(String.valueOf(second), 2, '0'));
        }
        else if (specifier == 'T') {
            format(buf, 'k', calendar);
            buf.append(':');
            format(buf, 'i', calendar);
            buf.append(':');
            format(buf, 's', calendar);
        }
        else if (specifier == 'u') {
            calendar.setFirstDayOfWeek(Calendar.MONDAY);
            calendar.setMinimalDaysInFirstWeek(4);
            // mysql week starts with 0, java week starts with 1
            int week;
            if (calendar.getWeekYear() != calendar.get(Calendar.YEAR)) {
                week = 0;
            }
            else {
                week = calendar.get(Calendar.WEEK_OF_YEAR);
            }
            buf.append(StringUtils.leftPad(String.valueOf(week), 2, '0'));
            calendar.setFirstDayOfWeek(Calendar.MONDAY);
        }
        else if (specifier == 'U') {
            calendar.setFirstDayOfWeek(Calendar.SUNDAY);
            calendar.setMinimalDaysInFirstWeek(7);
            // mysql week starts with 0, java week starts with 1
            int week;
            if (calendar.getWeekYear() != calendar.get(Calendar.YEAR)) {
                week = 0;
            }
            else {
                week = calendar.get(Calendar.WEEK_OF_YEAR);
            }
            buf.append(StringUtils.leftPad(String.valueOf(week), 2, '0'));
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
            calendar.setFirstDayOfWeek(Calendar.MONDAY);
            int year = calendar.get(Calendar.YEAR);
            if (calendar.get(Calendar.WEEK_OF_YEAR) == 1) year--;
            buf.append(StringUtils.leftPad(String.valueOf(year), 4, '0'));
        }
        else if (specifier == 'X') {
            throw new NotImplementedException();
        }
        else if (specifier == 'y') {
            int year = calendar.get(Calendar.YEAR) % 100;
            buf.append(StringUtils.leftPad(String.valueOf(year), 2, '0'));
        }
        else if (specifier == 'Y') {
            int year = calendar.get(Calendar.YEAR);
            buf.append(StringUtils.leftPad(String.valueOf(year), 4, '0'));
        }
        else if (specifier == '%') {
            buf.append('%');
        }
        else {
            buf.append(specifier);
        }
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
