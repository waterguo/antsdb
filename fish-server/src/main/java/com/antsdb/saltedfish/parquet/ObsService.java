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
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.MessageType;

import com.antsdb.saltedfish.nosql.StorageEngine;
import com.antsdb.saltedfish.nosql.StorageTable;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.obs.ObsProvider;
import com.antsdb.saltedfish.obs.action.UploadSet;
import com.antsdb.saltedfish.obs.cache.ObsCache;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.TableMeta;

/**
 * 
 * @author Frank Li<lizc@tg-hd.com>
 */
public interface ObsService extends StorageEngine {
    public final static String TABLE_SYNC_PARAM = "SYNCPARAM";

    void setMetaService(MetadataService metaService);

    TableName getTableName(int tableId);

    Mapping getMapping(int tableId);

    long getCommittedLogPointer();// upload log pointer;

    long getReplicateLogPointer();// sync log pointer;

    /**
     * get log point
     * @return
     */
    CheckPoint getCheckPoint();

    /**
     * update log pointer
     * @param sp
     * @throws IOException
     */
    void updateLogPointer(long sp) throws Exception;

    /**
     * wait sync 
     * @param i
     * @throws TimeoutException
     */
    void waitForSync(int i) throws TimeoutException;

    Map<String, Object> get_(String ns, String name, int tableId, byte[] key) throws Exception;

    /**
     * check table exists
     * @param namespace
     * @param tableName
     * @param tableId
     * @return
     */
    boolean existsTable(String namespace, String tableName, int tableId);

    TableMeta getTableMeta(int i);

    String getSystemNamespace();

    /**
     * fun local file dir
     * @return
     */
    File getLocalDataHome() throws Exception;

    String getRemoteDataHome() throws Exception;

    default void createDataAnalyticsDatabaseSchema(String namespace) {}

    default void dropDataAnalyticsDatabaseSchema(String namespace) {}

    default void dropDataAnalyticsTableSchema(String dbname, String tbname) {}

    default Configuration getHdfsConfiguration() {
        return new Configuration();
    }

    ObsProvider getStoreClient();
    
    default void startMerge(TableName tableInfo,Group group,MessageType schema,TableType tableType) throws Exception{
        
    }
    
    default void startCommit(long sp,UploadSet uploadSet) throws Exception{
        
    }

    void syncDropEntity(TableName delTableInfo);
    
    default void changeTableMerger(UploadSet uploadSet) throws Exception{
        
    }

    String getInitPartitiFile(int tableId);
    
    ObsCache getObsCache();
    ObsProvider getObsProvider();
    
    default Collection<StorageTable> getTableInfos(){
        return null;
    }

    void startBackup(String dest);
}
