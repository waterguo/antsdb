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
package com.antsdb.saltedfish.storage;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * @author *-xguo0<@
 */
public final class HBaseUtil {
    public static void closeQuietly(Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        }
        catch (Exception ignored) {}
    }
    
    public static Long getLong(Result r, String family, String column) {
        if (r == null) return null;
        byte[] bytes = r.getValue(Bytes.toBytes(family), Bytes.toBytes(column));
        return bytes != null ? Bytes.toLong(bytes) : null;
    }
    
    public static void set(Put put, String family, String column, Long value) {
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), value != null ? Bytes.toBytes(value) : null);
    }
}
