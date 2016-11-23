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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.antsdb.saltedfish.cpp.FishBool;
import com.antsdb.saltedfish.cpp.FishDate;
import com.antsdb.saltedfish.cpp.FishDecimal;
import com.antsdb.saltedfish.cpp.FishNumber;
import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.FishTime;
import com.antsdb.saltedfish.cpp.FishTimestamp;
import com.antsdb.saltedfish.cpp.FishUtf8;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Int4;
import com.antsdb.saltedfish.cpp.Int8;
import com.antsdb.saltedfish.cpp.Unicode16;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.sql.OrcaException;

/**
 * Provides arithmetic operations with appropriate implicit auto casting
 * 
 * @see http://dev.mysql.com/doc/refman/5.7/en/type-conversion.html
 * @author wgu0
 */
public class AutoCaster {
	static Pattern _ptnDate = Pattern.compile("(\\d+)-(\\d+)-(\\d+)");
	static Pattern _ptnTimestamp = Pattern.compile(
			"(\\d+)-(\\d+)-(\\d+) (\\d+):(\\d+):(\\d+)(\\.\\d+)?");
	static Pattern _ptnTimeDHMS = Pattern.compile(
			"(\\d+) (\\d+):(\\d+):(\\d+)");
	static Pattern _ptnTimeHMS = Pattern.compile(
			"(\\d{2}+)(\\d{2}+)(\\d{2}+)");
	static Pattern _ptnTimeHMSC = Pattern.compile(
			"(\\d+):(\\d+):(\\d+)(\\.(\\d+))?");
	/**
	 * WARNING: Integer.MIN_VALUE means NULL
	 * 
	 * @param heap
	 * @param addrx
	 * @param addry
	 * @return
	 */
	public static int compare(Heap heap, long addrx, long addry) {
		if ((addrx == 0) || (addry == 0)) {
			return Integer.MIN_VALUE;
		}
		int typex = Value.getType(heap, addrx);
		int typey = Value.getType(heap, addry);
		if (typex != typey) {
			int type = max(typex, typey);
			addrx = cast(heap, type, typex, addrx);
			addry = cast(heap, type, typey, addry);
		}
		if ((addrx == 0) || (addry == 0)) {
			return Integer.MIN_VALUE;
		}
		return FishObject.compare(heap, addrx, addry);
	}
	
	public static boolean equals(Heap heap, long addrx, long addry) {
		int compare = compare(heap, addrx, addry);
		return  compare == 0;
	}
	
	public static long plus(Heap heap, long addrx, long addry) {
		if ((addrx == 0) || (addry == 0)) {
			return 0;
		}
		int typex = Value.getType(heap, addrx);
		int typey = Value.getType(heap, addry);
		if (typex != typey) {
			int type = max(typex, typey);
			addrx = cast(heap, type, typex, addrx);
			addry = cast(heap, type, typey, addry);
		}
		return FishObject.plus(heap, addrx, addry);
	}
	
	public static long minus(Heap heap, long addrx, long addry) {
		if ((addrx == 0) || (addry == 0)) {
			return 0;
		}
		int typex = Value.getType(heap, addrx);
		int typey = Value.getType(heap, addry);
		if (typex != typey) {
			int type = max(typex, typey);
			addrx = cast(heap, type, typex, addrx);
			addry = cast(heap, type, typey, addry);
		}
		return FishObject.minus(heap, addrx, addry);
	}

	private final static int max(int typex, int typey) {
		int type;
		if ((typex == Value.TYPE_FLOAT) || (typey == Value.TYPE_FLOAT)) {
			type = Value.TYPE_FLOAT;
		}
		else if ((typex == Value.TYPE_NUMBER) || (typey == Value.TYPE_NUMBER)) {
			type = Value.TYPE_NUMBER;
		}
		else if ((typex == Value.TYPE_TIMESTAMP) || (typey == Value.TYPE_TIMESTAMP)) {
			type = Value.TYPE_TIMESTAMP;
		}
		else if ((typex == Value.TYPE_DATE) || (typey == Value.TYPE_DATE)) {
			type = Value.TYPE_DATE;
		}
		else if ((typex == Value.TYPE_BYTES) || (typey == Value.TYPE_BYTES)) {
			type = Value.TYPE_BYTES;
		}
		else {
			throw new IllegalArgumentException();
		}
		return type;
	}

	private final static long cast(Heap heap, int typeNew, int typeOld, long addr) {
		if (typeNew == typeOld) {
			return addr;
		}
		if (typeNew == Value.TYPE_FLOAT) {
			return FishObject.toFloat(heap, addr);
		}
		else if (typeNew == Value.TYPE_NUMBER) {
			return toNumber(heap, typeOld, addr);
		}
		else if (typeNew == Value.TYPE_TIMESTAMP) {
			return FishObject.toTimestamp(heap, addr);
		}
		else if (typeNew == Value.TYPE_DATE) {
			return FishObject.toDate(heap, addr);
		}
		else if (typeNew == Value.TYPE_BYTES) {
			return FishObject.toBytes(heap, addr);
		}
		else {
			throw new IllegalArgumentException();
		}
	}

	private static long toNumber(Heap heap, int currentType, long pValue) {
		switch (currentType) {
		case Value.TYPE_BOOL:
			boolean b = FishBool.get(heap, pValue);
			int value = b ? 1 : 0;
			return Int4.allocSet(heap, value);
		case Value.TYPE_STRING: {
			try {
				String s = (String)FishObject.get(heap, pValue);
				BigDecimal bd = new BigDecimal(s);
				long pp = FishNumber.allocSet(heap, bd);
				bd = (BigDecimal)FishObject.get(null, pp);
				return pp;
			}
			catch (Exception x) {
				return Int4.allocSet(heap, 0);
			}
		}
		default:
			return FishObject.toNumber(heap, pValue);	
		}
	}

	public static long toString(Heap heap, long pValue) {
		return FishObject.toString(heap, pValue);
	}
	
	public static long toNumber(Heap heap, long pValue) {
		if (pValue == 0) {
			return 0;
		}
		int type = Value.getType(heap, pValue);
		long p = cast(heap, Value.TYPE_NUMBER, type, pValue);
		return p;
	}

	public static Long getLong(long pValue) {
		byte format = Value.getFormat(null, pValue);
		switch (format) {
		case Value.FORMAT_INT4:
			return (long)Int4.get(pValue);
		case Value.FORMAT_INT8:
			return Int8.get(null, pValue);
		case Value.FORMAT_DECIMAL:
			BigDecimal bd = FishDecimal.get(null, pValue);
			return bd.longValueExact();
		case Value.FORMAT_DATE:
			Date dt = FishDate.get(null, pValue);
			return dt.getTime();
		case Value.FORMAT_TIMESTAMP:
			Timestamp ts = FishTimestamp.get(null, pValue);
			return ts.getTime();
		case Value.FORMAT_UTF8:
			// mysql converts illegal string to 0
			try {
				long n = Long.parseLong(FishUtf8.get(pValue));
				return n;
			}
			catch (Exception x) {
				return 0l;
			}
		case Value.FORMAT_UNICODE16:
			// mysql converts illegal string to 0
			try {
				return Long.parseLong(Unicode16.get(null, pValue));
			}
			catch (Exception x) {
				return 0l;
			}
		default:
			throw new IllegalArgumentException(String.valueOf(format));
		}
	}
	
	public static long toDate(Heap heap, long pValue) {
		if (pValue == 0) {
			return 0;
		}
		int type = Value.getType(heap, pValue);
		Date result;
		if (type == Value.TYPE_DATE) {
			return pValue;
		}
		else if (type == Value.TYPE_STRING) {
			String text = (String)FishObject.get(heap, pValue);
			result = parseDate(text);
			if (result == null) {
            	throw new OrcaException("invalid date value: " + text);
			}
		}
		else {
			throw new IllegalArgumentException();
		}
		return FishObject.allocSet(heap, result);
	}

	public static long toTimestamp(Heap heap, long pValue) {
		if (pValue == 0) {
			return 0;
		}
		int type = Value.getType(heap, pValue);
		Timestamp result;
		if (type == Value.TYPE_TIMESTAMP) {
			return pValue;
		}
		else if (type == Value.TYPE_STRING) {
			String text = (String)FishObject.get(heap, pValue);
			result = parseTimestamp(text);
			if (result == null) {
				Date dt = parseDate(text);
				if (dt != null) {
					result = new Timestamp(dt.getTime());
				}
				else {
	            	throw new OrcaException("invalid date value: " + text);
				}
			}
		}
		else {
			throw new IllegalArgumentException();
		}
		return FishObject.allocSet(heap, result);
	}
	
	@SuppressWarnings("deprecation")
	private static Timestamp parseTimestamp(String text) {
		Matcher m = _ptnTimestamp.matcher(text);
		if (m.find()) {
			int year = Integer.parseInt(m.group(1));
			int month = Integer.parseInt(m.group(2));
			int day = Integer.parseInt(m.group(3));
			int hour = Integer.parseInt(m.group(4));
			int minute = Integer.parseInt(m.group(5));
			int second = Integer.parseInt(m.group(6));
			String g7 = m.group(7);
			int milli = (g7 != null) ? Integer.parseInt(m.group(7).substring(1)) : 0;
			if ((year == 0) && (month == 0) && (day == 0)) {
				return new Timestamp(Long.MIN_VALUE);
			}
			else {
				return new Timestamp(year - 1900, month-1, day, hour, minute, second, milli * 1000000);
			}
		}
		return null;
	}
	
	@SuppressWarnings("deprecation")
	private static Date parseDate(String text) {
		Matcher m = _ptnDate.matcher(text);
		if (m.find()) {
			int year = Integer.parseInt(m.group(1));
			int month = Integer.parseInt(m.group(2));
			int day = Integer.parseInt(m.group(3));
			if ((year == 0) && (month == 0) && (day == 0)) {
				return new Date(Long.MIN_VALUE);
			}
			else {
				return new Date(year - 1900, month-1, day);
			}
		}
		return null;
	}

	public static long toBytes(Heap heap, long pValue) {
		int type = Value.getType(heap, pValue);
		if (type == Value.TYPE_BYTES) {
			return pValue;
		}
		else {
			throw new IllegalArgumentException();
		}
	}

	public static long toTime(Heap heap, long pValue) {
		if (pValue == 0) {
			return 0;
		}
		int type = Value.getType(heap, pValue);
		if (type == Value.TYPE_TIME) {
			return pValue;
		}
		if (type == Value.TYPE_NUMBER) {
			long time = FishNumber.longValue(pValue);
			return FishTime.allocSet(heap, time);
		}
		else if (type == Value.TYPE_STRING) {
			String text = (String)FishObject.get(heap, pValue);
			long time = parseTime(text);
			return FishTime.allocSet(heap, time);
		}
		else {
			throw new IllegalArgumentException();
		}
	}

	private static long parseTime(String text) {
		Matcher m = _ptnTimeHMSC.matcher(text);
		if (m.matches()) {
			long hh = Integer.parseInt(m.group(1));
			long mm = Integer.parseInt(m.group(2));
			long ss = Integer.parseInt(m.group(3));
			long sss = (m.group(5) == null) ? 0 : Integer.parseInt(m.group(5));
			return hh * 3600 * 1000 + mm * 60 * 1000 + ss * 1000 + sss;
		}
		m = _ptnTimeHMS.matcher(text);
		if (m.matches()) {
			long hh = Integer.parseInt(m.group(1));
			long mm = Integer.parseInt(m.group(2));
			long ss = Integer.parseInt(m.group(3));
			return hh * 3600 * 1000 + mm * 60 * 1000 + ss * 1000;
		}
		m = _ptnTimeDHMS.matcher(text);
		if (m.matches()) {
			long dd = Integer.parseInt(m.group(1));
			long hh = Integer.parseInt(m.group(2));
			long mm = Integer.parseInt(m.group(3));
			long ss = Integer.parseInt(m.group(4));
			return dd * 24 * 3600 * 1000 + hh * 3600 * 1000 + mm * 60 * 1000 + ss * 1000;
		}
		throw new IllegalArgumentException();
	}

	public static long negate(Heap heap, long pValue) {
		if (pValue == 0) {
			return 0;
		}
		int type = Value.getType(heap, pValue);
		if (type == Value.TYPE_NUMBER) {
			return FishNumber.negate(heap, pValue);
		}
		if (type == Value.TYPE_TIMESTAMP) {
			long epoch = FishTimestamp.getEpochMillisecond(heap, pValue);
			epoch = - epoch;
			return FishTimestamp.allocSet(heap, epoch);
		}
		throw new IllegalArgumentException();
	}

	public static long multiply(Heap heap, long pX, long pY) {
		if ((pX == 0) || (pY == 0)) {
			return 0;
		}
		int typex = Value.getType(heap, pX);
		int typey = Value.getType(heap, pY);
		if (typex != typey) {
			int type = max(typex, typey);
			pX = cast(heap, type, typex, pX);
			pY = cast(heap, type, typey, pY);
		}
		return FishObject.multiply(heap, pX, pY);
	}

	public static String getString(Heap heap, long pValue) {
		pValue = toString(heap, pValue);
		String value = (String)FishObject.get(heap, pValue);
		return value;
	}
}
