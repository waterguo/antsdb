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
package com.antsdb.saltedfish.cpp;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;

import com.antsdb.saltedfish.sql.vdm.AutoCaster;
import com.antsdb.saltedfish.sql.vdm.BlobReference;
import com.antsdb.saltedfish.sql.vdm.ToTimestampRelaxed;

public final class FishObject {

    public final static Object get(Heap heap, long addr) {
        if (addr == 0) {
            return null;
        }
        Object result = null;
        int type = Value.getFormat(null, addr);
        if (type == Value.FORMAT_INT4) {
            result = Int4.get(heap, addr);
        }
        else if (type == Value.FORMAT_INT8) {
            result = Int8.get(heap, addr);
        }
        else if (type == Value.FORMAT_BIGINT) {
            result = BigInt.get(heap, addr);
        }
        else if (type == Value.FORMAT_NULL) {
        }
        else if (type == Value.FORMAT_BOOL) {
            result = FishBool.get(heap, addr);
        }
        else if (type == Value.FORMAT_DECIMAL) {
            result = FishDecimal.get(heap, addr);
        }
        else if (type == Value.FORMAT_FAST_DECIMAL) {
            result = FastDecimal.get(heap, addr);
        }
        else if (type == Value.FORMAT_FLOAT4) {
            result = Float4.get(heap, addr);
        }
        else if (type == Value.FORMAT_FLOAT8) {
            result = Float8.get(heap, addr);
        }
        else if (type == Value.FORMAT_UTF8) {
            result = FishUtf8.get(addr);
        }
        else if (type == Value.FORMAT_UNICODE16) {
            result = Unicode16.get(heap, addr);
        }
        else if (type == Value.FORMAT_DATE) {
            result = FishDate.get(heap, addr);
        }
        else if (type == Value.FORMAT_TIMESTAMP) {
            result = FishTimestamp.get(heap, addr);
        }
        else if (type == Value.FORMAT_TIME) {
            result = FishTime.get(heap, addr);
        }
        else if (type == Value.FORMAT_BYTES) {
            result = Bytes.get(heap, addr);
        }
        else if (type == Value.FORMAT_KEY_BYTES) {
            result = KeyBytes.create(addr).get();
        }
        else if (type == Value.FORMAT_BOUNDARY) {
            result = new FishBoundary(addr);
        }
        else if (type == Value.FORMAT_INT4_ARRAY) {
            result = new Int4Array(addr).toArray();
        }
        else if (type == Value.FORMAT_BLOB_REF) {
            result = new BlobReference(addr);
        }
        else {
            throw new IllegalArgumentException(String.valueOf(type));
        }
        return result;
    }

    public final static long allocSet(Heap heap, Object value) {
        long addr = 0;
        if (value == null) {
            return 0;
        }
        else if (value instanceof BigInt) {
            addr = BigInt.allocSet(heap, (BigInteger)value);
        }
        else if (value instanceof Boolean) {
            addr = FishBool.allocSet(heap, (Boolean)value);
        }
        else if (value instanceof BigDecimal) {
            addr = FishNumber.allocSet(heap, (BigDecimal)value);
        }
        else if (value instanceof Double) {
            addr = Float8.allocSet(heap, (Double)value);
        }
        else if (value instanceof Float) {
            addr = Float4.allocSet(heap, (Float)value);
        }
        else if (value instanceof Integer) {
            addr = Int4.allocSet(heap, (Integer)value);
        }
        else if (value instanceof Long) {
            addr = Int8.allocSet(heap, (Long)value);
        }
        else if (value instanceof String) {
            addr = FishUtf8.allocSet(heap, (String)value);
        }
        else if (value instanceof Date) {
            addr = FishDate.allocSet(heap, (Date)value);
        }
        else if (value instanceof Timestamp) {
            addr = FishTimestamp.allocSet(heap, (Timestamp)value);
        }
        else if (value instanceof byte[]) {
            addr = Bytes.allocSet(heap, (byte[])value);
        }
        else if (value instanceof int[]) {
            addr = Int4Array.alloc(heap, (int[])value).getAddress();
        }
        else if (value instanceof BigInteger) {
            addr = BigInt.allocSet(heap, (BigInteger)value);
        }
        else {
            throw new IllegalArgumentException();
        }
        return addr;
    }
    
    public final static int compare(Heap heap, long xAddr, long yAddr) {
        byte type1 = Unsafe.getByte(xAddr);
        byte type2 = Unsafe.getByte(yAddr);
        
        // both are null
        
        if ((type1 | type2) == 0) {
            return 0;
        }
        
        // one of them is null
        
        if ((type1 != Value.TYPE_NULL) && (type2 == Value.TYPE_NULL)) {
            return 1;
        }
        if ((type1 == Value.TYPE_NULL) && (type2 != Value.TYPE_NULL)) {
            return -1;
        }
        
        // test kind
        
        byte kind1 = Value.getType(type1);
        byte kind2 = Value.getType(type2);
        if (kind1 != kind2) {
            throw new IllegalArgumentException();
        }
        
        // ok go ahead diving into types
        
        switch(kind1) {
        case Value.TYPE_NUMBER:
            return FishNumber.compare(xAddr, yAddr);
        case Value.TYPE_STRING:
            return FishString.compare(xAddr, yAddr);
        case Value.TYPE_BYTES:
            return Bytes.compare(xAddr, yAddr);
        case Value.TYPE_DATE:
            return FishDate.compare(xAddr, yAddr);
        case Value.TYPE_TIMESTAMP:
            return FishTimestamp.compare(xAddr, yAddr);
        default:
            throw new IllegalArgumentException();
        }
    }

    public final static boolean equals(Heap heap, long addrx, long addry) {
        return compare(heap, addrx, addry) == 0;
    }

    public static long plus(Heap heap, long addrx, long addry) {
        if ((addrx == 0) || (addry == 0)) {
            throw new IllegalArgumentException();
        }
        byte typex = Value.getType(heap, addrx);
        byte typey = Value.getType(heap, addry);
        if (typex != typey) {
            throw new IllegalArgumentException();
        }
        if (typex == Value.TYPE_NUMBER) {
            return FishNumber.add(heap, addrx, addry);
        }
        else if (typex == Value.TYPE_TIMESTAMP) {
            return FishTimestamp.add(heap, addrx, addry);
        }
        else {
            throw new IllegalArgumentException();    
        }
    }

    public static long multiply(Heap heap, long addrx, long addry) {
        if ((addrx == 0) || (addry == 0)) {
            throw new IllegalArgumentException();
        }
        byte typex = Value.getType(heap, addrx);
        byte typey = Value.getType(heap, addry);
        if (typex != typey) {
            throw new IllegalArgumentException();
        }
        if (typex == Value.TYPE_NUMBER) {
            return FishNumber.multiply(heap, addrx, addry);
        }
        else {
            throw new IllegalArgumentException();    
        }
    }
    
    public static long minus(Heap heap, long addrx, long addry) {
        throw new IllegalArgumentException();    
    }

    public static long toFloat(Heap heap, long addr) {
        byte format = Value.getFormat(heap, addr);
        double value;
        switch (format) {
        case Value.FORMAT_FLOAT4:
            value = Float4.get(heap, addr);
            break;
        case Value.FORMAT_FLOAT8:
            value = Float8.get(heap, addr);
            break;
        case Value.FORMAT_INT4:
            value = Int4.get(addr);
            break;
        case Value.FORMAT_INT8:
            value = Int8.get(heap, addr);
            break;
        case Value.FORMAT_DECIMAL: {
            BigDecimal bd = FishDecimal.get(heap, addr);
            value = bd.doubleValue();
            break;
        }
        default:
            throw new IllegalArgumentException(String.valueOf(format));
        }
        return Float8.allocSet(heap, value);
    }

    public static long toNumber(Heap heap, long addr) {
        throw new IllegalArgumentException();    
    }

    public static long toTimestamp(Heap heap, long addr) {
        return ToTimestampRelaxed.toTimestamp(heap, addr, Integer.MAX_VALUE);
    }

    public static long toDate(Heap heap, long addr) {
        byte format = Value.getFormat(heap, addr);
        switch (format) {
        case Value.FORMAT_DATE:
            return addr;
        case Value.FORMAT_UTF8: {
            String s = FishUtf8.getString(addr);
            long value = Date.valueOf(s).getTime();
            return FishDate.allocSet(heap, value);
        }
        case Value.FORMAT_UNICODE16:
            String s = Unicode16.get(null, addr);
            long value = Date.valueOf(s).getTime();
            return FishDate.allocSet(heap, value);
        default:
            throw new IllegalArgumentException();
        }
    }
    
    public static BigInteger toBigInteger(Heap heap, long addr) {
        byte format = Value.getFormat(heap, addr);
        switch (format) {
        case Value.FORMAT_INT4:
            return BigInteger.valueOf(Int4.get(addr));
        case Value.FORMAT_INT8:
            return BigInteger.valueOf(Int8.get(heap, addr));
        case Value.FORMAT_BIGINT:
            return BigInt.get(heap, addr);
        case Value.FORMAT_DECIMAL:
            BigDecimal bd = FishDecimal.get(heap, addr);
            return bd.toBigIntegerExact();
        case Value.FORMAT_DATE:
            return BigInteger.valueOf(FishDate.getEpochMillisecond(heap, addr));
        case Value.FORMAT_TIMESTAMP:
            return BigInteger.valueOf(FishTimestamp.getEpochMillisecond(heap, addr));
        case Value.FORMAT_UTF8:
            try {
                return new BigInteger(FishUtf8.get(addr));
            }
            catch (Exception x) {
                // mysql treats illegal string literal as 0
                return BigInteger.ZERO;
            }
        case Value.FORMAT_UNICODE16:
            try {
                return new BigInteger(Unicode16.get(null, addr));
            }
            catch (Exception x) {
                // mysql treats illegal string literal as 0
                return BigInteger.ZERO;
            }
        default:
            throw new IllegalArgumentException(String.valueOf(format));
        }
    }
    
    public static BigDecimal toBigDecimal(Heap heap, long addr) {
        if (addr == 0) return null;
        byte format = Value.getFormat(heap, addr);
        if (format == Value.FORMAT_DECIMAL) {
            return FishDecimal.get(heap, addr);
        }
        if (format == Value.FORMAT_FAST_DECIMAL) {
            return FastDecimal.get(heap, addr);
        }
        if (format == Value.FORMAT_FLOAT4) {
            return BigDecimal.valueOf(Float4.get(heap, addr));
        }
        if (format == Value.FORMAT_FLOAT8) {
            return BigDecimal.valueOf(Float8.get(heap, addr));
        }
        return new BigDecimal(toBigInteger(heap, addr));
    }
    
    public static long toLong(Heap heap, long addr) {
        byte format = Value.getFormat(heap, addr);
        switch (format) {
        case Value.FORMAT_INT4:
            return Int4.get(addr);
        case Value.FORMAT_INT8:
            return Int8.get(heap, addr);
        case Value.FORMAT_BIGINT:
            return BigInt.get(heap, addr).longValueExact();
        case Value.FORMAT_FAST_DECIMAL:
            BigDecimal bd1 = FastDecimal.get(heap, addr);
            return bd1.longValueExact();
        case Value.FORMAT_DECIMAL:
            BigDecimal bd = FishDecimal.get(heap, addr);
            return bd.longValueExact();
        case Value.FORMAT_DATE:
            return FishDate.getEpochMillisecond(heap, addr);
        case Value.FORMAT_TIMESTAMP:
            return FishTimestamp.getEpochMillisecond(heap, addr);
        case Value.FORMAT_UTF8:
            try {
                long n = Long.parseLong(FishUtf8.get(addr));
                return n;
            }
            catch (Exception x) {
                // mysql treats illegal string literal as 0
                return 0;
            }
        case Value.FORMAT_UNICODE16:
            try {
                return Long.parseLong(Unicode16.get(null, addr));
            }
            catch (Exception x) {
                // mysql treats illegal string literal as 0
                return 0;
            }
        default:
            throw new IllegalArgumentException(String.valueOf(format));
        }
    }
    
    public final static long toBytes(Heap heap, long addr) {
        byte format = Value.getFormat(heap, addr);
        switch (format) {
        case Value.FORMAT_BYTES:
            return addr;
        case Value.FORMAT_UTF8:
            return FishUtf8.toBytes(heap, format, addr);
        case Value.FORMAT_UNICODE16:
            return Unicode16.toBytes(heap, format, addr);
        default:
            throw new IllegalArgumentException();
        }
    }

    public final static BrutalMemoryObject getBrutalObject(long pValue) {
        if (pValue == 0) {
            return null;
        }
        byte format = Value.getFormat(null, pValue);
        switch (format) {
        case Value.FORMAT_INT4:
            return new Int4(pValue);
        case Value.FORMAT_INT8:
            return new Int8(pValue);
        case Value.FORMAT_BIGINT:
            return new BigInt(pValue);
        case Value.FORMAT_UTF8:
            return new FishUtf8(pValue);
        case Value.FORMAT_UNICODE16:
            return new Unicode16(pValue);
        case Value.FORMAT_KEY_BYTES:
            return new KeyBytes(pValue);
        case Value.FORMAT_BYTES:
            return new Bytes(pValue);
        case Value.FORMAT_BOOL:
            return new FishBool(pValue);
        case Value.FORMAT_DECIMAL:
            return new FishDecimal(pValue);
        case Value.FORMAT_TIMESTAMP:
            return new FishTimestamp(pValue);
        case Value.FORMAT_TIME:
            return new FishTime(pValue);
        case Value.FORMAT_FLOAT4:
            return new Float4(pValue);
        case Value.FORMAT_FLOAT8:
            return new Float8(pValue);
        case Value.FORMAT_DATE:
            return new FishDate(pValue);
        case Value.FORMAT_NULL:
            return null;
        case Value.FORMAT_INT4_ARRAY:
            return new Int4Array(pValue);
        case Value.FORMAT_BLOB_REF:
            return new BlobReference(pValue);
        default:
            throw new IllegalArgumentException();
        }
    }
    
    public final static int getSize(long pValue) {
        if (pValue == 0) {
            return 0;
        }
        byte format = Value.getFormat(null, pValue);
        switch (format) {
        case Value.FORMAT_INT4:
            return Int4.getSize();
        case Value.FORMAT_INT8:
            return Int8.getSize();
        case Value.FORMAT_UTF8:
            return FishUtf8.getSize(format, pValue);
        case Value.FORMAT_UNICODE16:
            return Unicode16.getSize(format, pValue);
        case Value.FORMAT_KEY_BYTES:
            return KeyBytes.getRawSize(pValue);
        case Value.FORMAT_BYTES:
            return Bytes.getRawSize(pValue);
        case Value.FORMAT_BOOL:
            return FishBool.getSize(pValue);
        case Value.FORMAT_DECIMAL:
            return FishDecimal.getSize(pValue);
        case Value.FORMAT_TIMESTAMP:
            return FishTimestamp.getSize(pValue);
        case Value.FORMAT_TIME:
            return FishTime.getSize(pValue);
        case Value.FORMAT_FLOAT4:
            return Float4.getSize(pValue);
        case Value.FORMAT_FLOAT8:
            return Float8.getSize(pValue);
        case Value.FORMAT_DATE:
            return FishDate.getSize(pValue);
        case Value.FORMAT_NULL:
            return 1;
        case Value.FORMAT_INT4_ARRAY:
            return new Int4Array(pValue).getSize();
        case Value.FORMAT_BLOB_REF:
            return new BlobReference(pValue).getSize();
        default:
            BrutalMemoryObject bmo = getBrutalObject(pValue);
            if (bmo != null) {
                return bmo.getByteSize();
            }
            else {
                throw new IllegalArgumentException();
            }
        }
    }

    public static long toUtf8(Heap heap, long pValue) {
        if (pValue == 0) {
            return 0;
        }
        byte format = Value.getFormat(null, pValue);
        if (format != Value.FORMAT_UTF8) {
            if (format != Value.FORMAT_UNICODE16) {
                pValue = toString(heap, pValue);
            }
            if (format == Value.FORMAT_UNICODE16) {
                pValue = Unicode16.toUtf8(heap, pValue);
            }
        }
        return pValue;
    }
    
    public static long toString(Heap heap, long pValue) {
        return AutoCaster.toString(heap, pValue);
    }
    
    public static String debug(long pValue) {
        if (pValue == 0) {
            return "NULL";
        }
        StringBuilder buf = new StringBuilder();
        int format = Value.getFormat(null, pValue);
        buf.append(String.format("[%02x]", format & 0xff));
        switch (format) {
        case Value.FORMAT_INT4:
            buf.append(Int4.get(pValue));
            break;
        case Value.FORMAT_INT8:
            buf.append(Int8.get(null, pValue));
            break;
        case Value.FORMAT_DECIMAL:
            buf.append(FishDecimal.get(null, pValue));
            break;
        case Value.FORMAT_UNICODE16:
            buf.append(Unicode16.get(null, pValue));
            break;
        case Value.FORMAT_BYTES:
            buf.append(Bytes.toString(pValue));
            break;
        case Value.FORMAT_KEY_BYTES:
            buf.append(KeyBytes.create(pValue).toString());
            break;
        case Value.FORMAT_FLOAT4:
            buf.append(Float4.get(null, pValue));
            break;
        case Value.FORMAT_FLOAT8:
            buf.append(Float8.get(null, pValue));
            break;
        case Value.FORMAT_BLOB_REF:
            buf.append("BLOBREF SIZE=");
            buf.append(new BlobReference(pValue).getDataSize());
            break;
        default:
            Object obj = FishObject.get(null, pValue);
            buf.append(obj.toString());
        }
        return buf.toString();
    }

    public static long clone(Heap heap, long pValue) {
        if (pValue == 0) {
            return 0;
        }
        int size = FishObject.getSize(pValue);
        long pResult = heap.alloc(size);
        Unsafe.copyMemory(pValue, pResult, size);
        return pResult;
    }
}
