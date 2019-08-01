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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.cpp.Bytes;
import com.antsdb.saltedfish.cpp.FastDecimal;
import com.antsdb.saltedfish.cpp.FishBool;
import com.antsdb.saltedfish.cpp.FishDate;
import com.antsdb.saltedfish.cpp.FishDecimal;
import com.antsdb.saltedfish.cpp.FishNumber;
import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.FishTime;
import com.antsdb.saltedfish.cpp.FishTimestamp;
import com.antsdb.saltedfish.cpp.FishUtf8;
import com.antsdb.saltedfish.cpp.Float4;
import com.antsdb.saltedfish.cpp.Float8;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Int4;
import com.antsdb.saltedfish.cpp.Int4Array;
import com.antsdb.saltedfish.cpp.Int8;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.cpp.Unicode16;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.sql.OrcaException;
import com.google.common.base.Charsets;

/**
 * Provides arithmetic operations with appropriate implicit auto casting
 * 
 * @see http://dev.mysql.com/doc/refman/5.7/en/type-conversion.html
 * @author wgu0
 */
public class AutoCaster {
    static Pattern _ptnDate = Pattern.compile("(\\d+)-(\\d+)-(\\d+)");
    static Pattern _ptnTimestamp = Pattern.compile(
            "(\\d+)[-|/](\\d+)[-|/](\\d+)( (\\d+)[:|/](\\d+)[:|/](\\d+))?(\\.(\\d{1,6}))?");
    static Pattern _ptnTimeDHMS = Pattern.compile(
            "(\\d+) (\\d+):(\\d+):(\\d+)");
    static Pattern _ptnTimeHMS = Pattern.compile(
            "(\\d{2}+)(\\d{2}+)(\\d{2}+)");
    static Pattern _ptnTimeHMSC = Pattern.compile(
            "-?(\\d+):(\\d+):(\\d+)(\\.(\\d+))?");
    static final int[] TIME_FRACTION_SCALE = new int[] {100000000, 10000000, 1000000, 100000, 10000, 1000};

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
        long result = upcast(heap, addrx, addry, (px, py) -> {
            return FishObject.plus(heap, px, py);
        });
        return result;
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
        if ((typex == Value.TYPE_NUMBER) || (typey == Value.TYPE_NUMBER)) {
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

    public final static long cast(Heap heap, int typeNew, int typeOld, long addr) {
        if (typeNew == typeOld) {
            return addr;
        }
        if (typeNew == Value.TYPE_NUMBER) {
            return toNumber(heap, typeOld, addr);
        }
        else if (typeNew == Value.TYPE_TIMESTAMP) {
            return toTimestamp(heap, addr);
        }
        else if (typeNew == Value.TYPE_DATE) {
            return toDate(heap, addr);
        }
        else if (typeNew == Value.TYPE_BYTES) {
            return FishObject.toBytes(heap, addr);
        }
        else if (typeNew == Value.TYPE_STRING) {
            return toString(heap, addr);
        }
        else {
            throw new IllegalArgumentException();
        }
    }

    private static long toNumber(Heap heap, int currentType, long pValue) {
        switch (currentType) {
        case Value.TYPE_NUMBER:
            return pValue;
        case Value.TYPE_BOOL:
            boolean b = FishBool.get(heap, pValue);
            int value = b ? 1 : 0;
            return Int4.allocSet(heap, value);
        case Value.TYPE_BYTES:
            return toNumber(heap, Value.TYPE_STRING, toString(heap, pValue));
        case Value.TYPE_STRING:
            return parseNumber(heap, pValue);
        case Value.FORMAT_TIMESTAMP:
            return Int8.allocSet(heap, getLong(pValue));
        case Value.FORMAT_DATE:
            return Int8.allocSet(heap, getLong(pValue));
        default:
            return FishObject.toNumber(heap, pValue);    
        }
    }

    private static long parseNumber(Heap heap, long pValue) {
        String s = (String)FishObject.get(heap, pValue);
        try {
            long result = Long.parseLong(s);
            return Int8.allocSet(heap, result);
        }
        catch (Exception x) {
        }
        try {
            BigDecimal result = new BigDecimal(s);
            return FishNumber.allocSet(heap, result);
        }
        catch (Exception x) {
        }
        try {
            double result = Double.parseDouble(s);
            return Float8.allocSet(heap, result);
        }
        catch (Exception x) {
        }
        return Int4.allocSet(heap, 0);
    }

    public static long toString(Heap heap, long pValue) {
        if (pValue == 0) {
            return 0;
        }
        byte format = Value.getFormat(null, pValue);
        switch (format) {
        case Value.FORMAT_UTF8:
            return pValue;
        case Value.FORMAT_UNICODE16:
            return pValue;
        case Value.FORMAT_INT4: {
            long value = Int4.get(heap, pValue);
            return Unicode16.allocSet(heap, String.valueOf(value));
        }
        case Value.FORMAT_INT8: {
            long value = Int8.get(heap, pValue);
            return Unicode16.allocSet(heap, String.valueOf(value));
        }
        case Value.FORMAT_DATE: {
            return FishUtf8.allocSet(heap, FishDate.get(heap, pValue).toString());
        }
        case Value.FORMAT_TIME: {
            return FishUtf8.allocSet(heap, FishTime.get(heap, pValue).toString());
        }
        case Value.FORMAT_TIMESTAMP: {
            return FishUtf8.allocSet(heap, FishTimestamp.get(heap, pValue).toString());
        }
        case Value.FORMAT_DECIMAL: {
            BigDecimal value = FishDecimal.get(heap, pValue);
            return Unicode16.allocSet(heap, value.toString());
        }
        case Value.FORMAT_FAST_DECIMAL: {
            BigDecimal value = FastDecimal.get(heap, pValue);
            return Unicode16.allocSet(heap, value.toString());
        }
        case Value.FORMAT_BYTES: {
            byte[] bytes = Bytes.get(heap, pValue);
            return Unicode16.allocSet(heap, new String(bytes, Charsets.UTF_8));
        }
        case Value.FORMAT_KEY_BYTES: {
            return Unicode16.allocSet(heap, KeyBytes.create(pValue).toString());
        }
        case Value.FORMAT_BOOL: {
            boolean value = FishBool.get(heap, pValue);
            return Unicode16.allocSet(heap, value ? "1" : "0");
        }
        case Value.FORMAT_INT4_ARRAY: {
            Int4Array array = new Int4Array(pValue);
            String temp = StringUtils.join(ArrayUtils.toObject(array.toArray()), ",");
            return Unicode16.allocSet(heap, temp);
        }
        default:
            throw new IllegalArgumentException(String.valueOf(format));
        }
    }
    
    public static long toNumber(Heap heap, long pValue) {
        if (pValue == 0) {
            return 0;
        }
        int type = Value.getType(heap, pValue);
        long p = cast(heap, Value.TYPE_NUMBER, type, pValue);
        return p;
    }

    public static int getInt(long pValue) {
        byte format = Value.getFormat(null, pValue);
        if (format == Value.FORMAT_INT4) {
            return Int4.get(pValue);
        }
        long result = getLong(pValue);
        return (int)result;
    }
    
    @SuppressWarnings("deprecation")
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
        case Value.FORMAT_DATE: {
            Date val = FishDate.get(null, pValue);
            long result;
            if (val.getTime() == Long.MIN_VALUE) {
                // mysql 0000-00-00
                result = 0;
            }
            else {
                result = val.getYear() + 1900;
                result = result * 100 + val.getMonth() + 1;
                result = result * 100 + val.getDate();
            }
            return result;
        }
        case Value.FORMAT_TIMESTAMP: {
            Timestamp val = FishTimestamp.get(null, pValue);
            if (val == null) {
                return null;
            }
            long result;
            if (val.getTime() == Long.MIN_VALUE) {
                // mysql 0000-00-00 00:00:00
                result = 0;
            }
            else {
                result = val.getYear() + 1900;
                result = result * 100 + val.getMonth() + 1;
                result = result * 100 + val.getDate();
                result = result * 100 + val.getHours();
                result = result * 100 + val.getMinutes();
                result = result * 100 + val.getSeconds();
            }
            return result;
        }
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
    
    @SuppressWarnings("deprecation")
    public static long toDate(Heap heap, long pValue) {
        if (pValue == 0) {
            return 0;
        }
        int type = Value.getType(heap, pValue);
        Date result;
        if (type == Value.TYPE_DATE) {
            return pValue;
        }
        else if (type == Value.TYPE_TIMESTAMP) {
            Timestamp value = FishTimestamp.get(heap, pValue);
            result = new Date(value.getYear(), value.getMonth(), value.getDate());
        }
        else if (type == Value.TYPE_STRING) {
            String text = (String)FishObject.get(heap, pValue);
            result = parseDate(text);
            if (result == null) {
                    throw new OrcaException("invalid date value: " + text);
            }
        }
        else if (type == Value.TYPE_BYTES) {
            pValue = toString(heap, pValue);
            return toDate(heap, pValue);
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
        else if (type == Value.TYPE_DATE) {
            long epoch = FishDate.getEpochMillisecond(heap, pValue);
            return FishTimestamp.allocSet(heap, epoch);
        }
        else if (type == Value.TYPE_STRING) {
            String text = (String)FishObject.get(heap, pValue);
            if (StringUtils.isBlank(text)) {
                return 0;
            }
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
        else if (type == Value.FORMAT_BYTES) {
            return toTimestamp(heap, toString(heap, pValue));
        }
        else if (type == Value.TYPE_NUMBER) {
            long epoch = FishNumber.longValue(pValue);
            return FishTimestamp.allocSet(heap, epoch);
        }
        else {
            throw new IllegalArgumentException();
        }
        return FishObject.allocSet(heap, result);
    }
    
    private static Timestamp parseTimestamp(String text) {
        Timestamp result;
        result = parseTimestamp1(text);
        if (result == null) {
            result = parseTimestamp2(text);
        }
        return result;
    }
    
    /**
     * yyyyMMddHHmmss format
     * @param text
     * @return
     */
    @SuppressWarnings("deprecation")
    private static Timestamp parseTimestamp2(String text) {
        if (text.length() != 14) {
            return null;
        }
        try {
            int year = Integer.parseInt(text.substring(0, 4));
            int month = Integer.parseInt(text.substring(4, 6));
            int day = Integer.parseInt(text.substring(6, 8));
            int hour = Integer.parseInt(text.substring(8, 10));
            int minute = Integer.parseInt(text.substring(10, 12));
            int second = Integer.parseInt(text.substring(12, 14));
            return new Timestamp(year - 1900, month-1, day, hour, minute, second, 0);
        }
        catch (Exception x) {
            return null;
        }
    }
    
    /**
     * yyyy-MM-dd HH:mm:ss.sss format
     * @param text
     * @return
     */
    @SuppressWarnings("deprecation")
    private static Timestamp parseTimestamp1(String text) {
        Matcher m = _ptnTimestamp.matcher(text);
        if (m.find()) {
            int year = Integer.parseInt(m.group(1));
            int month = Integer.parseInt(m.group(2));
            int day = Integer.parseInt(m.group(3));
            int hour = 0, minute = 0, second = 0, nano = 0;
            if (m.group(4) != null) {
                hour = Integer.parseInt(m.group(5));
                minute = Integer.parseInt(m.group(6));
                second = Integer.parseInt(m.group(7));
                String g8 = m.group(9);
                if (g8 != null) {
                    int g7len = g8.length();
                    nano = Integer.parseInt(m.group(8).substring(1)) * TIME_FRACTION_SCALE[g7len - 1];
                }
            }
            if ((year == 0) && (month == 0) && (day == 0)) {
                return new Timestamp(Long.MIN_VALUE);
            }
            else {
                return new Timestamp(year - 1900, month-1, day, hour, minute, second, nano);
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
        else if (type == Value.FORMAT_BYTES) {
            return toTime(heap, toString(heap, pValue));
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
            long result = hh * 3600 * 1000 + mm * 60 * 1000 + ss * 1000 + sss; 
            if (text.startsWith("-")) {
                result = -result;
            }
            return result;
        }
        m = _ptnTimeHMS.matcher(text);
        if (m.matches()) {
            long hh = Integer.parseInt(m.group(1));
            long mm = Integer.parseInt(m.group(2));
            long ss = Integer.parseInt(m.group(3));
            long result = hh * 3600 * 1000 + mm * 60 * 1000 + ss * 1000;
            if (text.startsWith("-")) {
                result = -result;
            }
            return result;
        }
        m = _ptnTimeDHMS.matcher(text);
        if (m.matches()) {
            long dd = Integer.parseInt(m.group(1));
            long hh = Integer.parseInt(m.group(2));
            long mm = Integer.parseInt(m.group(3));
            long ss = Integer.parseInt(m.group(4));
            long result = dd * 24 * 3600 * 1000 + hh * 3600 * 1000 + mm * 60 * 1000 + ss * 1000;
            if (text.startsWith("-")) {
                result = -result;
            }
            return result;
        }
        throw new IllegalArgumentException(text);
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
        if (typex != Value.TYPE_NUMBER) {
            pX = cast(heap, Value.TYPE_NUMBER, typex, pX);
            typex = Value.TYPE_NUMBER;
        }
        int typey = Value.getType(heap, pY);
        if (typey != Value.TYPE_NUMBER) {
            pY = cast(heap, Value.TYPE_NUMBER, typey, pY);
            typey = Value.TYPE_NUMBER;
        }
        return FishObject.multiply(heap, pX, pY);
    }

    public static String getString(Heap heap, long pValue) {
        pValue = toString(heap, pValue);
        String value = (String)FishObject.get(heap, pValue);
        return value;
    }

    public static double getDouble(long pValue) {
        byte format = Value.getFormat(null, pValue);
        switch (format) {
        case Value.FORMAT_INT4:
            return (double)Int4.get(pValue);
        case Value.FORMAT_INT8:
            return (double)Int8.get(null, pValue);
        case Value.FORMAT_FLOAT8:
            return (double)Float8.get(null, pValue);
        case Value.FORMAT_DECIMAL:
            BigDecimal bd = FishDecimal.get(null, pValue);
            return bd.doubleValue();
        case Value.FORMAT_DATE:
            Date dt = FishDate.get(null, pValue);
            return dt.getTime();
        case Value.FORMAT_TIMESTAMP:
            Timestamp ts = FishTimestamp.get(null, pValue);
            return ts.getTime();
        case Value.FORMAT_UTF8:
            // mysql converts illegal string to 0
            try {
                double n = Double.parseDouble(FishUtf8.get(pValue));
                return n;
            }
            catch (Exception x) {
                return 0l;
            }
        case Value.FORMAT_UNICODE16:
            // mysql converts illegal string to 0
            try {
                double n = Double.parseDouble(Unicode16.get(null, pValue));
                return n;
            }
            catch (Exception x) {
                return 0l;
            }
        default:
            throw new IllegalArgumentException(String.valueOf(format));
        }
    }
    
    private static final long upcast(Heap heap, long px, long py, BiFunction<Long, Long, Long> func) {
        if ((px == 0) || (py == 0)) {
            return 0;
        }
        int typex = Value.getType(heap, px);
        int typey = Value.getType(heap, py);
        if (typex != Value.TYPE_NUMBER) {
            px = cast(heap, Value.TYPE_NUMBER, typex, px);
            typex = Value.TYPE_NUMBER;
        }
        if (typey != Value.TYPE_NUMBER) {
            py = cast(heap, Value.TYPE_NUMBER, typey, py);
            typey = Value.TYPE_NUMBER;
        }
        if ((px == 0) || (py == 0)) {
            return 0;
        }
        long result = func.apply(px, py); 
        return result;
    }
    
    public static long addTime(Heap heap, long px, long py) {
        if ((px == 0) || (py == 0)) {
            return 0;
        }
        px = AutoCaster.toTimestamp(heap, px);
        py = AutoCaster.toTimestamp(heap, py);
        if ((px == 0) || (py == 0)) {
            return 0;
        }
        long x = FishTimestamp.getEpochMillisecond(heap, px);
        if (x == Long.MIN_VALUE) {
            // '0000-00-00 00:00:00' + interval '0' second returns NULL
            return 0;
        }
        long y = FishTimestamp.getEpochMillisecond(heap, py);
        if (y == Long.MIN_VALUE) {
            // '0000-00-00 00:00:00' + interval '0' second returns NULL
            return 0;
        }
        long z = x + y;
        long pResult = FishTimestamp.allocSet(heap, z);
        return pResult;
    }

    public static BigDecimal getDecimal(long pValue) {
        BigDecimal result;
        byte format = Value.getFormat(null, pValue);
        switch (format) {
        case Value.FORMAT_INT4:
            result = BigDecimal.valueOf(Int4.get(pValue));
            break;
        case Value.FORMAT_INT8:
            result = BigDecimal.valueOf(Int8.get(null, pValue));
            break;
        case Value.FORMAT_FAST_DECIMAL:
            result = FastDecimal.get(null, pValue);
            break;
        case Value.FORMAT_DECIMAL:
            result = FishDecimal.get(null, pValue);
            break;
        case Value.FORMAT_FLOAT4:
            result = BigDecimal.valueOf(Float4.get(null, pValue));
            break;
        case Value.FORMAT_FLOAT8:
            result = BigDecimal.valueOf(Float8.get(null, pValue));
            break;
        case Value.FORMAT_UTF8:
            // mysql converts illegal string to 0
            try {
                result = new BigDecimal(FishUtf8.get(pValue));
            }
            catch (Exception x) {
                result = BigDecimal.valueOf(0);
            }
        case Value.FORMAT_UNICODE16:
            // mysql converts illegal string to 0
            try {
                result = new BigDecimal(Unicode16.get(null, pValue));
            }
            catch (Exception x) {
                result = BigDecimal.valueOf(0);
            }
        default:
            throw new IllegalArgumentException(String.valueOf(format));
        }
        return result;
    }

    /*
     * @see https://dev.mysql.com/doc/refman/5.7/en/arithmetic-functions.html
     */
    public static long divide(Heap heap, long pX, long pY) {
        long result = upcast(heap, pX, pY, (px, py) -> {
            BigDecimal x = getDecimal(px);
            BigDecimal y = getDecimal(py);
            if (y.equals(BigDecimal.ZERO)) {
                return 0l;
            }
            BigDecimal z = x.divide(y, x.scale() + 4, RoundingMode.HALF_DOWN);
            return FishNumber.allocSet(heap, z);
        });
        return result;
    }

    /**
     * DO NOT USE. NOT FULLY IMPLEMNETED YET
     */
    public static long cast(Heap heap, long pValue, int format) {
        // input checks
        
        if (pValue == 0) {
            return 0;
        }
        int curFormat = Value.getFormat(heap, pValue);
        if (curFormat == format) {
            return pValue;
        }
        
        // convert type
        
        int type = Value.getType(format);
        pValue = cast(heap, type, Value.getType(heap, pValue), pValue);
        if (pValue == 0) {
            return 0;
        }
        if (curFormat == format) {
            return pValue;
        }
        
        // convert format
        
        switch(format) {
            case Value.FORMAT_INT4 : {
            }
            case Value.FORMAT_INT8 :
            case Value.FORMAT_BIGINT :
            case Value.FORMAT_FAST_DECIMAL :
            case Value.FORMAT_DECIMAL :
            case Value.FORMAT_FLOAT4 :
            case Value.FORMAT_FLOAT8 :
            case Value.FORMAT_TIME :
            case Value.FORMAT_DATE :
            case Value.FORMAT_TIMESTAMP :
            case Value.FORMAT_UTF8 :
            case Value.FORMAT_UNICODE16 :
            case Value.FORMAT_BOOL :
            case Value.FORMAT_BYTES :
            default:
                throw new IllegalArgumentException(String.valueOf(format));
        }
    }

    public static Boolean getBoolean(long pValue) {
        if (pValue == 0) {
            return null;
        }
        byte format = Value.getFormat(null, pValue);
        if (format == Value.FORMAT_BOOL) {
            return FishBool.get(null, pValue);
        }
        Long result = getLong(pValue);
        return result != null ? result==1 : null;
    }
}
