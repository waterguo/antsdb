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

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;

import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.util.UberUtil;
import com.google.common.primitives.UnsignedBytes;

/**
 * 
 * - Lexicographic. to be compatible with hbase
 * 
 * @author xguo
 *
 */
class KeyUtil {
    /**
     * return the number of bytes that the key uses 
     * 
     * @param columns
     * @return
     */
    static int getSize(List<ColumnMeta> columns) {
        int size = 0;
        for (ColumnMeta i:columns) {
            DataType type = i.getDataType();
            if (type.getJavaType() == Integer.class) {
                size += 4;
            }
            else if (type.getJavaType() == Long.class) {
                size += 8;
            }
            else {
                throw new NotImplementedException(type + " is not supported to particiate in index");
            }
        }
        return size;
    }
    
    static void toBytes(List<ColumnMeta> columns, byte[] bytes, List<Object> values, byte filler) {
        int pos = 0;
        for (int i=0; i<columns.size(); i++) {
            ColumnMeta column = columns.get(i);
            if (i < values.size()) {
                Object value = values.get(i);
                pos += write(column, bytes, pos, value);
            }
            else {
                Arrays.fill(bytes, pos, bytes.length, filler);
            }
        }
    }
    
    /**
     * 
     * @param column
     * @param bytes
     * @param pos
     * @param obj
     * @return number of bytes written
     */
    static int write(ColumnMeta column, byte[] bytes, int pos, Object obj) {
        DataType type = column.getDataType();
        if (obj == null) {
            throw new NotImplementedException("null is not supported as part of key for now");
        }
        if (type.getJavaType() == Integer.class) {
            Integer value = 0;
            if (obj != null) {
                value = UberUtil.toObject(Integer.class, obj);
            }
            if (value < 0) {
                throw new NotImplementedException("negtive number is not yet supported");
            }
            bytes[pos+0] = (byte) (value >> 24);
            bytes[pos+1] = (byte) (value >> 16);
            bytes[pos+2] = (byte) (value >> 8);
            bytes[pos+3] = (byte) (int)value;
            return 4;
        }
        else if (type.getJavaType() == Long.class) {
            Long value = 0l;
            if (obj != null) {
                value = UberUtil.toObject(Long.class, obj);
            }
            if (value < 0) {
                throw new NotImplementedException("negtive number is not yet supported");
            }
            for (int i = 7; i >= 0; i--) {
                bytes[pos+i] = (byte) (value & 0xffL);
                value >>= 8;
            }
            return 8;
        }
        else {
            throw new NotImplementedException(type + " is not supported to particiate in index");
        }
    }
    
    public static void add(byte[] bytes, byte n) {
        int overflow = 0;
        for (int i = bytes.length-1; i >= 0; i--) {
            int v = (bytes[i] & 0xff) + n + overflow;
            bytes[i] = (byte) v;
            overflow = v >>> 8;
            if (overflow == 0) {
                break;
            }
            n = 0;
        }
        if (overflow > 0) {
            Arrays.fill(bytes, (byte)0xff);
        }
        else if (overflow < 0) {
            Arrays.fill(bytes, (byte)0);
        }
    }
    
    public static byte[] max(byte[] value1, byte[] value2) {
        if (value1 == null) {
            return value2;
        }
        else if (value2 == null) {
            return value1;
        }
        else {
            int result = UnsignedBytes.lexicographicalComparator().compare(value1, value2);
            return (result >= 0) ? value1 : value2;
        }
    }

    public static byte[] min(byte[] value1, byte[] value2) {
        if (value1 == null) {
            return value2;
        }
        else if (value2 == null) {
            return value1;
        }
        else {
            int result = UnsignedBytes.lexicographicalComparator().compare(value1, value2);
            return (result <= 0) ? value1 : value2;
        }
    }
}
