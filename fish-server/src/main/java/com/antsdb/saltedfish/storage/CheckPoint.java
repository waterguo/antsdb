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
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.antsdb.saltedfish.sql.Orca;

/**
 * 
 * @author wgu0
 */
public class CheckPoint {
    final static byte[] COLUMN_FAMILY = Bytes.toBytes("d");
    final static byte[] TRUNCATE_KEY = Bytes.toBytes(1);
    
    /** space pointer that has been synchronized */
    volatile long currentSp;
    long serverId;
    private boolean isMutable;
    private long createTimestamp;
    private long updateTimestamp;
    private String createOrcaVersion;
    private String updateorcaVersion;
    private boolean isActive;
    
    CheckPoint(boolean isMutable) throws IOException {
        this.isMutable = isMutable;
    }
    
    public long getCurrentSp() {
        return currentSp;
    }

    public static Map<Long, CheckPoint> loadAll(Connection conn, String ns) throws Exception {
        // Get table object
        Table table = conn.getTable(TableName.valueOf(ns, HBaseStorageService.TABLE_SYNC_PARAM));
        
        // scan
        Map<Long, CheckPoint> result = new TreeMap<>();
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (;;) {
            Result r = scanner.next();
            if (r == null) break;
            if (Arrays.equals(r.getRow(), Bytes.toBytes(0))) {
                // this is the anchor row
                continue;
            }
            CheckPoint i = new CheckPoint(true);
            i.read(r);
            result.put(i.serverId, i);
        }
        return result;
    }
    
    private void read(Result result) {
        this.currentSp = Bytes.toLong(get(result, "currentSp"));
        this.createTimestamp = Optional.ofNullable(get(result, "createTimestamp")).map(Bytes::toLong).orElse(0l);
        this.updateTimestamp = Optional.ofNullable(get(result, "updateTimestamp")).map(Bytes::toLong).orElse(0l);
        this.createOrcaVersion = Bytes.toString(get(result, "createOrcaVersion"));
        this.updateorcaVersion = Bytes.toString(get(result, "updateOrcaVersion"));
        this.isActive = Optional.ofNullable(get(result, "isActive")).map(Bytes::toBoolean).orElse(Boolean.FALSE);
    }

    public void load(Connection conn, String ns, long serverId) throws IOException {
        // Get table object
        Table table = conn.getTable(TableName.valueOf(ns, HBaseStorageService.TABLE_SYNC_PARAM));
        
        // Query row
        this.serverId = serverId;
        Get get = new Get(Bytes.toBytes(serverId));
        Result result = table.get(get);
        if (!result.isEmpty()) {
            read(result);
        }
    }
    
    /**
     * save changes to hbase
     * @throws IOException 
     */
    public void save(Connection conn, String ns) throws IOException {
        if (!this.isMutable) {
            throw new OrcaHBaseException("failed to update since it is realy only mode");
        }
        // Get table object
        Table table = conn.getTable(TableName.valueOf(ns, HBaseStorageService.TABLE_SYNC_PARAM));
        
        // Generate put data
        Put put = new Put(Bytes.toBytes(this.serverId));
        set(put, "currentSp", this.currentSp);
        if (this.createTimestamp == 0l) {
            this.createTimestamp = System.currentTimeMillis();
        }
        this.updateTimestamp = System.currentTimeMillis();
        if (this.createOrcaVersion == null) {
            this.createOrcaVersion = Orca._version;
        }
        this.updateorcaVersion = Orca._version;
        set(put, "createTimestamp", this.createTimestamp);
        set(put, "updateTimestamp", this.updateTimestamp);
        set(put, "createOrcaVersion", this.createOrcaVersion);
        set(put, "updateOrcaVersion", this.updateorcaVersion);
        set(put, "isActive", this.isActive);
        
        // put row
        table.put(put);
    }

    private byte[] get(Result r, String column) {
        byte[] result = r.getValue(COLUMN_FAMILY, Bytes.toBytes(column));
        return result;
    }
    
    private void set(Put put, String column, Object value) {
        byte[] bytes;
        if (value instanceof Long) {
            bytes = Bytes.toBytes((Long)value);
        }
        else if (value instanceof String) {
            bytes = Bytes.toBytes((String)value);
        }
        else if (value instanceof Boolean) {
            bytes = Bytes.toBytes((Boolean)value);
        }
        else {
            throw new IllegalArgumentException();
        }
        put.addColumn(COLUMN_FAMILY, Bytes.toBytes(column), bytes);
    }
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append(String.format("sever id: %d", this.serverId));
        buf.append(String.format("\nlog pointer: %x", getCurrentSp()));
        buf.append(String.format("\ncreate orca version: %s", this.createOrcaVersion));
        buf.append(String.format("\nupdate orca version: %s", this.updateorcaVersion));
        buf.append(String.format("\ncreate timestamp: %s", new Timestamp(this.createTimestamp).toString()));
        buf.append(String.format("\nupdate timestamp: %s", new Timestamp(this.updateTimestamp).toString()));
        buf.append(String.format("\nactive: %b", this.isActive));
        return buf.toString();
    }

    public void setActive(boolean b) {
        this.isActive = true;
    }

    public void setLogPointer(long value) {
        this.currentSp = value;
    }
}
