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
package com.antsdb.saltedfish.parquet.bean;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.util.Bytes;

import com.antsdb.saltedfish.parquet.ParquetUtils;
import com.google.gson.annotations.Expose;

public class Partition implements java.lang.Comparable<Partition> {
    @Expose
    private final static String VERSION_FORMAT = "-%08x";
    @Expose
    public final static String DATA_FILE_ALL_FORMAT = "%08x-%08x-%08x%s";//<table id>-<partition id>-<version id>.par
    @Expose
    public final static String DATA_FILE_FORMAT = "%08x-%08x";//<table id>-<partition id>
    @Expose
    public final static byte[] defaultStartKey = new byte[] { 00, 00, 00, 00, 00, 00, 00, 00 };
    @Expose
    public final static byte[] defaultEndKey = new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
            (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff };
    
    private long id = -1;
    private byte[] startKey;
    private byte[] endKey;
    private String dbName;
    private String tableName;
    private int tableId;
    private long rowCount = 0;
    private long dataSize = 0;
    private long createTimestamp;
    private long lastAccessTimestamp;
    private long remoteTimestamp;

    @Expose
    private int status;
    
    private AtomicInteger version = new AtomicInteger(0); //data file version
    
    public String getDataFileName() {
        return String.format(DATA_FILE_FORMAT, 
                tableId, 
                id
                );
    }
    
    public String getAllFileNameByIndex() {
        return getDataFileName() + ParquetUtils.DATA_PARQUET_EXT_NAME;
    }
    
    public Partition(String dbName,String tableName,Integer tableId) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.tableId = tableId;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public void setStartKey(byte[] startKey) {
        this.startKey = startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

    public void setEndKey(byte[] endKey) {
        this.endKey = endKey;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public long getRowCount() {
        return rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public long getDataSize() {
        return dataSize;
    }

    public void setDataSize(long dataSize) {
        this.dataSize = dataSize;
    }
 
    public int getVersion() {
        return version.get();
    }

    public int getTableId() {
        return tableId;
    }
    
    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public void setCreateTimestamp(long createTimestamp) {
        this.createTimestamp = createTimestamp;
    }

    public long getLastAccessTimestamp() {
        return lastAccessTimestamp;
    }

    public void setLastAccessTimestamp(long lastAccessTimestamp) {
        this.lastAccessTimestamp = lastAccessTimestamp;
    }

    public long getRemoteTimestamp() {
        return remoteTimestamp;
    }

    public void setRemoteTimestamp(long remoteTimestamp) {
        this.remoteTimestamp = remoteTimestamp;
    }

    public String getNextVersion() {
        int tmp = version.incrementAndGet();
        return String.format(VERSION_FORMAT, tmp);
    }
    public int nextVersion() {
        int nextVersion = version.incrementAndGet();
        return nextVersion;
    }

    @Override
    public int compareTo(Partition other) {
        if (other == null) {
            return 1;
        }
        if (other instanceof Partition) {
            byte[] localRowkey = getStartKey();
            byte[] otherRowkey = ((Partition) other).getStartKey();

            if (localRowkey == null) {
                return 0;
            }
            else if (otherRowkey == null) {
                return 1;
            }
            long result = Bytes.compareTo(localRowkey, otherRowkey);
            if (result == 0) {
                Long localId = getId();
                Long otherId = ((Partition) other).getId();
                return localId.compareTo(otherId);
            }
            return (int) result;
        }
        return 1;
    }

    @Override
    public int hashCode() {
        if (getStartKey() != null) {
            return getStartKey().hashCode();
        }
        else {
            return super.hashCode();
        }
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    /**
     * partition change check use
     */
    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        }
        if (other instanceof Partition) {
            Long localId = getId();
            Partition otherIndex = (Partition) other;
            Long otherId = otherIndex.getId();
            return localId.equals(otherId) 
                    && this.getTableId() == (otherIndex.getTableId())
                    ;
        }
        return false;
    }
    
    
    public String getVersionFileName() {
        return String.format(DATA_FILE_ALL_FORMAT, 
                this.getTableId(), 
                this.getId(),
                this.getVersion(),
                ParquetUtils.DATA_PARQUET_EXT_NAME
                );
    }
    
    public String getNextVersionFileName() {
        return String.format(DATA_FILE_ALL_FORMAT, 
                this.getTableId(), 
                this.getId(),
                this.nextVersion(),
                ParquetUtils.DATA_PARQUET_EXT_NAME
                );
    }
    
    public String getMergeFileName() {
        return String.format(DATA_FILE_ALL_FORMAT, 
                this.getTableId(), 
                this.getId(),
                this.getVersion(),
                ParquetUtils.MERGE_PARQUET_EXT_NAME
                );
    }

    public String toLog() {
        return String.format("pId=%s tableId=%s startKey=%s endKey=%s rows=%s size=%s v={}", 
                this.id,
                this.tableId,
                Bytes.toHex(this.startKey),
                Bytes.toHex(this.endKey),
                this.rowCount,
                this.dataSize,
                this.version);
    }

}
