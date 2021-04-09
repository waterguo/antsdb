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

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * this class is used to bind a hbase name space to a antsdb instance  
 * @author *-xguo0<@
 */
public class Anchor {
    private final static String FAMILY = "d";
    
    /** should be same as instanceId from Humpback. used to prevent accidental sync*/
    public Long instanceId;
    
    public static Anchor load(Connection conn, String ns) throws IOException {
        Table table = conn.getTable(TableName.valueOf(ns, HBaseStorageService.TABLE_SYNC_PARAM));
        Result result = table.get(new Get(Bytes.toBytes(0)));
        if (result.isEmpty()) {
            return null;
        }
        Anchor r = new Anchor();
        r.instanceId = HBaseUtil.getLong(result, FAMILY, "serverId");
        return r;
    }
    
    public void save(Connection conn, String ns) throws IOException {
        Table table = conn.getTable(TableName.valueOf(ns, HBaseStorageService.TABLE_SYNC_PARAM));
        Put put = new Put(Bytes.toBytes(0));
        HBaseUtil.set(put, FAMILY, "serverId", this.instanceId);
        table.put(put);
    }
}
