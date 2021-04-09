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

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Optional;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;

import com.antsdb.saltedfish.obs.ObsProvider;
import com.antsdb.saltedfish.obs.cache.ObsCache;
import com.antsdb.saltedfish.obs.cache.ObsFileReference;
import com.antsdb.saltedfish.obs.hdfs.HdfsStorageService;
import com.antsdb.saltedfish.parquet.bean.SyncParam;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author Frank Li<lizc@tg-hd.com>
 */
public class CheckPoint {
    static Logger _log = UberUtil.getThisLogger();
 
    final static byte[] KEY = Bytes.toBytes(0);
    final static byte[] TRUNCATE_KEY = Bytes.toBytes(1);
    /** space pointer that has been synchronized */
    volatile long currentSp;
    /** should be same as serverId from Humpback. used to prevent accidental sync*/
    long serverId;
    private TableName tn;
    private long createTimestamp;
    private long updateTimestamp;
    private String createOrcaVersion;
    private String updateorcaVersion;
    private boolean isActive;
    private boolean isMutable;
    
    public CheckPoint(TableName  tn,boolean isMutable) throws IOException {
        this.tn = tn;
        this.isMutable = isMutable;
    }
    
    public long getCurrentSp() {
        return currentSp;
    }
    
    public long getServerId() {
        return serverId;
    }

    public void setServerId(long value) {
        this.serverId = value;
    }
    
    public SyncParam readSyncParam(ObsCache obsCache) throws IOException {
        SyncParam result = readSyncParam(obsCache,this.tn);
        if (result!=null) {
            this.currentSp = result.getCurrentSp();
            this.serverId = result.getServerId();
            this.createTimestamp = Optional.ofNullable(result.getCreateTimestamp()).orElse(0l);
            this.updateTimestamp = Optional.ofNullable(result.getUpdateTimestamp()).orElse(0l);
            this.createOrcaVersion =result.getCreateOrcaVersion();
            this.updateorcaVersion = result.getUpdateorcaVersion();
            this.isActive =Optional.ofNullable(result.isActive()).orElse(Boolean.FALSE);
        }
        return result;
    }
    
    /**
     * no event save syncparam table datta
     * @param conn
     * @throws IOException
     */
    public SyncParam createSyncParam() throws IOException {
        if (!this.isMutable) {
            throw new OrcaObjectStoreException("obs storage is in read-only mode");
        }
        
        // Get table object   
        SyncParam row = new SyncParam();
        if (this.createTimestamp == 0l) {
            this.createTimestamp = System.currentTimeMillis();
        }
        this.updateTimestamp = System.currentTimeMillis();
        if (this.createOrcaVersion == null) {
            this.createOrcaVersion = Orca._version;
        }
        this.updateorcaVersion = Orca._version;
        row.setCurrentSp(this.currentSp);
        row.setServerId(this.serverId);
        row.setCreateTimestamp(this.createTimestamp);
        row.setUpdateTimestamp(this.updateTimestamp);
        row.setCreateOrcaVersion(this.createOrcaVersion);
        row.setUpdateorcaVersion(this.updateorcaVersion);
        row.setActive( this.isActive);
        
        _log.trace("parquet file create or append!!! tablename is: {},currentSp:{}",tn,currentSp);
        return row;
    }
    
    /**
     * save changes to object store
     * use event save syncparam table datta
     * @throws IOException 
     */
    public SyncParam updateSyncParam(long lp) throws IOException {
        if (!this.isMutable) {
            throw new OrcaObjectStoreException("obs storage is in read-only mode");
        }
        if(lp == 0 || lp >= this.currentSp) {
            _log.trace("obs parquet updateLogPointer: currentSp:{} to new :{}",currentSp,lp);
            SyncParam row = new SyncParam();
            if (this.createTimestamp == 0l) {
                this.createTimestamp = System.currentTimeMillis();
            }
            this.updateTimestamp = System.currentTimeMillis();
            if (this.createOrcaVersion == null) {
                this.createOrcaVersion = Orca._version;
            }
            this.updateorcaVersion = Orca._version;
           
            row.setCurrentSp(lp);
            row.setServerId(this.serverId);
            row.setCreateTimestamp(this.createTimestamp);
            row.setUpdateTimestamp(this.updateTimestamp);
            row.setCreateOrcaVersion(this.createOrcaVersion);
            row.setUpdateorcaVersion(this.updateorcaVersion);
            row.setActive( this.isActive);
            
            _log.trace("obs parquet file update,tablename is: {},currentSp:{},new lp:{}",tn,currentSp,lp);
            return row;
        }
        else {
            _log.warn("current sp {} less then {},skip!!!",lp,currentSp);
        }
       return null;
    }
   
    public SyncParam updateSyncParam() throws IOException {
        if (!this.isMutable) {
            throw new OrcaObjectStoreException("obs storage is in read-only mode");
        }
        SyncParam row = new SyncParam();
        if (this.createTimestamp == 0l) {
            this.createTimestamp = System.currentTimeMillis();
        }
        this.updateTimestamp = System.currentTimeMillis();
        if (this.createOrcaVersion == null) {
            this.createOrcaVersion = Orca._version;
        }
        this.updateorcaVersion = Orca._version;
       
        row.setCurrentSp(this.currentSp);
        row.setServerId(this.serverId);
        row.setCreateTimestamp(this.createTimestamp);
        row.setUpdateTimestamp(this.updateTimestamp);
        row.setCreateOrcaVersion(this.createOrcaVersion);
        row.setUpdateorcaVersion(this.updateorcaVersion);
        row.setActive( this.isActive);
        
        _log.trace("obs parquet file update,tablename is: {},currentSp:{}",tn,currentSp);
        return row;
    }
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append(String.format("sever id: %d", getServerId()));
        buf.append(String.format("\nlog pointer: %x", getCurrentSp()));
        buf.append(String.format("\ncreate orca version: %s", this.createOrcaVersion));
        buf.append(String.format("\nupdate orca version: %s", this.updateorcaVersion));
        buf.append(String.format("\ncreate timestamp: %s", new Timestamp(this.createTimestamp).toString()));
        buf.append(String.format("\nupdate timestamp: %s", new Timestamp(this.updateTimestamp).toString()));
        buf.append(String.format("\nactive: %b", this.isActive));
        return buf.toString();
    }

    public void setActive(boolean b) {
        this.isActive = b;
    }

    public void setLogPointer(long value) {
        this.currentSp = value;
    }
    
    private SyncParam readSyncParam(ObsCache obsCache,TableName tableName) {
        if (HdfsStorageService.TABLE_SYNC_PARAM.equals(tableName.getTableName())) {
            SyncParam object = new SyncParam();
            ObsFileReference parquetFile = null;
            try {
                String objectKey = tableName.getTableName() + ParquetUtils.DATA_JSON_EXT_NAME;
              
                parquetFile = obsCache.get(objectKey);
                if(parquetFile!=null) {
                    long fileSize = parquetFile.getFsize();
                    if (fileSize > ParquetDataWriter.minParquetSize) {
                        File file = parquetFile.getFile();
                        String contents = FileUtils.readFileToString(file);
                        object = UberUtil.toObject(contents, SyncParam.class);
                        return object;
                    }
                }
            }
            catch (Exception e) {
                _log.error(e.getMessage(), e);
                throw new OrcaObjectStoreException(e);
            }
            finally {
                if(parquetFile != null) {
                    parquetFile.release();
                    parquetFile = null;
                }
            }
        }
        return null;
    }

    public SyncParam writeSyncParam(File localDataHome, 
            String remoteDataHome, 
            ObsProvider provider,
            ObsCache obsCache) throws Exception {
        if (!this.isMutable) {
            throw new OrcaObjectStoreException("obs storage is in read-only mode");
        }
     // Get table object   
        SyncParam row = new SyncParam();
        if (this.createTimestamp == 0l) {
            this.createTimestamp = System.currentTimeMillis();
        }
        this.updateTimestamp = System.currentTimeMillis();
        if (this.createOrcaVersion == null) {
            this.createOrcaVersion = Orca._version;
        }
        this.updateorcaVersion = Orca._version;
        row.setCurrentSp(this.currentSp);
        row.setServerId(this.serverId);
        row.setCreateTimestamp(this.createTimestamp);
        row.setUpdateTimestamp(this.updateTimestamp);
        row.setCreateOrcaVersion(this.createOrcaVersion);
        row.setUpdateorcaVersion(this.updateorcaVersion);
        row.setActive( this.isActive);

        File file = new File(localDataHome,tn.getTableName() + ParquetUtils.DATA_JSON_EXT_NAME);
        String data = UberUtil.toJson(row);

        FileUtils.writeStringToFile(file, data);

        String objectKey = tn.getTableName() + ParquetUtils.DATA_JSON_EXT_NAME;

        provider.uploadFile(remoteDataHome + objectKey, file.getAbsolutePath(), file.length());
        _log.trace("objectKey:{},localPath:{}", objectKey, localDataHome);
        
        return row;
    }
}
