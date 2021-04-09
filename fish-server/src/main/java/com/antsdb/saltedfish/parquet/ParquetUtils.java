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
package com.antsdb.saltedfish.parquet;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Date;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.parquet.io.api.Binary;

import com.antsdb.saltedfish.cpp.FishBoundary;
import com.antsdb.saltedfish.sql.vdm.BlobReference;

/**
 * 
 * @author Frank Li<lizc@tg-hd.com>
 */
public class ParquetUtils {
    
    public final static String DATA_PARQUET_EXT_NAME = ".par";
    public final static String DATA_JSON_EXT_NAME = ".json";
    public final static String MERGE_PARQUET_EXT_NAME = ".merge";

    public byte[] toByte(Object val) {
        byte[] result = null;
        if (val == null) {
            return result;
        }
        else if (val instanceof Integer) {
            Integer objVal = (Integer) val;
            result = Bytes.toBytes(objVal);
        }
        else if (val instanceof Long) {
            Long objVal = (Long) val;
            result = Bytes.toBytes(objVal);
        }
        else if (val instanceof Boolean) {
            Boolean objVal = (Boolean) val;
            result = Bytes.toBytes(objVal);
        }
        else if (val instanceof String) {
            String objVal = (String) val;
            result = Bytes.toBytes(objVal);
        }
        else if (val instanceof BigDecimal) {
            BigDecimal objVal = (BigDecimal) val;
            result = Bytes.toBytes(objVal);
        }
        else if (val instanceof Float) {
            Float objVal = (Float) val;
            result = Bytes.toBytes(objVal);
        }
        else if (val instanceof Double) {
            Double objVal = (Double) val;
            result = Bytes.toBytes(objVal);
        }
        else if (val instanceof Date) {
            Date d = (Date) val;
            result = Bytes.toBytes(d.getTime());
        }
        else if (val instanceof Time) {
            Time t = (Time) val;
            result = Bytes.toBytes(t.getTime());
        }
        else if (val instanceof Timestamp) {
            Timestamp d = (Timestamp) val;
            result = Bytes.toBytes(d.getTime());
        }
        else if (val instanceof Duration) {
            Duration d = (Duration) val;
            result = Bytes.toBytes(d.toMillis());
        }
        else if (val instanceof byte[]) {
            result = (byte[]) val;
        }
        else if (val instanceof FishBoundary) {
            FishBoundary d = (FishBoundary) val;
            result = Bytes.toBytes(d.toString());
        }
        else if (val instanceof int[]) {
            int[] d = (int[]) val;
            if (d != null && d.length > 0) {
                result = Bytes.toBytes(d[0]);
            }
        }
        else if (val instanceof BlobReference) {
            BlobReference d = (BlobReference) val;
            result = Bytes.toBytes(d.toString());
        }
        else {
            throw new IllegalArgumentException(String.valueOf(val));
        }
        return result;
    }

    public static Binary toBinary(Object val) {
        byte[] result = null;
        if (val == null) {
            return null;
        }
        else if (val instanceof Integer) {
            Integer objVal = (Integer) val;
            result = Bytes.toBytes(objVal);
        }
        else if (val instanceof Long) {
            Long objVal = (Long) val;
            result = Bytes.toBytes(objVal);
        }
        else if (val instanceof Boolean) {
            Boolean objVal = (Boolean) val;
            result = Bytes.toBytes(objVal);
        }
        else if (val instanceof String) {
            String objVal = (String) val;
            result = Bytes.toBytes(objVal);
        }
        else if (val instanceof BigDecimal) {
            BigDecimal objVal = (BigDecimal) val;
            byte[] decimalBytes = objVal.setScale(5).unscaledValue().toByteArray();
            int precToBytes = MessageTypeSchemaUtils.FIXED_LEN;
            result = new byte[precToBytes];
            System.arraycopy(decimalBytes, 0, result, precToBytes - decimalBytes.length, decimalBytes.length); // Padding leading zeroes/ones.
        }
        else if (val instanceof BigInteger) {
            BigInteger objVal = (BigInteger) val;
            byte[] bigResult = objVal.toByteArray();
            
            int precToBytes = MessageTypeSchemaUtils.FIXED_LEN;
            result = new byte[precToBytes];
            System.arraycopy(bigResult, 0, result, precToBytes - bigResult.length, bigResult.length); // Padding leading zeroes/ones.
        }
        else if (val instanceof Float) {
            Float objVal = (Float) val;
            result = Bytes.toBytes(objVal);
        }
        else if (val instanceof Double) {
            Double objVal = (Double) val;
            result = Bytes.toBytes(objVal);
        }
        else if (val instanceof Date) {
            Date d = (Date) val;
            result = Bytes.toBytes(d.getTime());
        }
        else if (val instanceof Time) {
            Time t = (Time) val;
            result = Bytes.toBytes(t.getTime());
        }
        else if (val instanceof Timestamp) {
            Timestamp d = (Timestamp) val;
            result = Bytes.toBytes(d.getTime());
        }
        else if (val instanceof Duration) {
            Duration d = (Duration) val;
            result = Bytes.toBytes(d.toMillis());
        }
        else if (val instanceof byte[]) {
            result = (byte[]) val;
        }
        else if (val instanceof FishBoundary) {
            FishBoundary d = (FishBoundary) val;
            result = Bytes.toBytes(d.toString());
        }
        else if (val instanceof int[]) {
            int[] d = (int[]) val;
            ByteBuffer buffer = ByteBuffer.allocate(d.length * 4);
            for (int tmp : d) {
                buffer.putInt(tmp);
            }
            result = buffer.array();
            buffer.clear();
            buffer = null;
        }
        else if (val instanceof BlobReference) {
            BlobReference d = (BlobReference) val;
            result = Bytes.toBytes(d.toString());
        }
        else {
            throw new IllegalArgumentException(String.valueOf(val));
        }
        if (result == null) {
            return null;
        }
        Binary binaryResult = Binary.fromReusedByteArray(result);
        return binaryResult;
    }

    public static Long toInt64(Object val) {
        Long result = null;
        if (val == null) {
            return result;
        }
        else if (val instanceof Long) {
            Long objVal = (Long) val;
            result = objVal;
        }
        else if (val instanceof Integer) {
            Integer objVal = (Integer) val;
            result = Long.valueOf(objVal);
        }
        else {
            throw new IllegalArgumentException(String.valueOf(val));
        }
        return result;
    }
    
    public static Long toInt96(Object val) {
        Long result = null;
        if (val == null) {
            return result;
        }
        else if (val instanceof Long) {
            Long objVal = (Long) val;
            result = objVal;
        }
        else if (val instanceof Integer) {
            Integer objVal = (Integer) val;
            result = Long.valueOf(objVal);
        }
        else if (val instanceof BigInteger) {
            BigInteger objVal = (BigInteger) val;
            result = Long.valueOf(objVal.longValue());
        }
        else {
            throw new IllegalArgumentException(String.valueOf(val));
        }
        return result;
    }

    public static Integer toInt32(Object val) {
        Integer result = null;
        if (val == null) {
            return result;
        }
        else if (val instanceof Integer) {
            Integer objVal = (Integer) val;
            result = objVal;
        }
        else if (val instanceof Long) {
            Long objVal = (Long) val;
            result = Integer.valueOf(objVal.intValue());
        }
        else {
            throw new IllegalArgumentException(String.valueOf(val));
        }
        return result;
    }

    public static Boolean toBoolean(Object val) {
        Boolean result = null;
        if (val == null) {
            return result;
        }
        else if (val instanceof Boolean) {
            Boolean objVal = (Boolean) val;
            result = objVal;
        }
        else if (val instanceof Integer) {
            Integer objVal = (Integer) val;
            result = objVal.intValue() == 0 ? false : true;
        }
        else if (val instanceof Long) {
            Long objVal = (Long) val;
            result = objVal.intValue() == 0 ? false : true;
        }
        else {
            throw new IllegalArgumentException(String.valueOf(val));
        }
        return result;
    }

    public static Double toDouble(Object val) {
        Double result = null;
        if (val == null) {
            return result;
        }
        else if (val instanceof Double) {
            Double objVal = (Double) val;
            result = objVal;
        }
        else {
            throw new IllegalArgumentException(String.valueOf(val));
        }
        return result;
    }

    public static Float toFloat(Object val) {
        Float result = null;
        if (val == null) {
            return result;
        }
        else if (val instanceof Float) {
            Float objVal = (Float) val;
            result = objVal;
        }
        else {
            throw new IllegalArgumentException(String.valueOf(val));
        }
        return result;
    }

    public static Long toInt64ByDatetime(Object val) {
        Long result = null;
        if (val == null) {
            return null;
        }
        if (val instanceof Date) {
            Date d = (Date) val;
            result = d.getTime();
        }
        else if (val instanceof Time) {
            Time t = (Time) val;
            result = t.getTime();
        }
        else if (val instanceof Timestamp) {
            Timestamp d = (Timestamp) val;
            result = d.getTime();
        }
        else if (val instanceof Duration) {
            Duration d = (Duration) val;
            result = d.toMillis();
        }
        else {
            throw new IllegalArgumentException(String.valueOf(val));
        }
        return result;
    }

}
