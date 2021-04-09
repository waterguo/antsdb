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
package com.antsdb.saltedfish.obs.aliyun;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.nosql.ConfigService;
import com.antsdb.saltedfish.nosql.HColumnRow;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.HumpbackSession;
import com.antsdb.saltedfish.nosql.SpaceManager;
import com.antsdb.saltedfish.obs.upload.ExecutorBoatPool;
import com.antsdb.saltedfish.obs.upload.ExecutorCatchPool;
import com.antsdb.saltedfish.parquet.BaseStorageService;
import com.antsdb.saltedfish.parquet.MessageTypeSchemaUtils;
import com.antsdb.saltedfish.parquet.OrcaObjectStoreException;
import com.antsdb.saltedfish.parquet.ParquetReplicationHandler;
import com.antsdb.saltedfish.parquet.TableName;
import com.antsdb.saltedfish.util.UberTime;
import com.antsdb.saltedfish.util.UberTimer;
import com.antsdb.saltedfish.util.UberUtil;

/**
 *
 * @author Frank Li<lizc@tg-hd.com>
 */
public class OssStorageService extends BaseStorageService {
    static Logger _log = UberUtil.getThisLogger();

    private String sourceBucketName;

    private boolean dlaEnable = false;
    private String dlaUrl;
    private String dlaUser;
    private String dlaPasswd;
    
    public OssStorageService(Humpback humpback) throws Exception {
        this.humpback = humpback;
    }

    @Override
    public void open(File home, ConfigService antsdbConfig, boolean isMutable) throws Exception {
        this.isMutable = isMutable;
        
        this.home = home;
        this.maxColumnPerPut = antsdbConfig.getHBaseMaxColumnsPerPut();
        String compressCodec = antsdbConfig.getDatafileCompressionCodec();
        this.compressionType = CompressionCodecName.valueOf(compressCodec.toUpperCase());
        this.sysns = antsdbConfig.getSystemNamespace();
        _log.debug("system namespace: {}", this.sysns);
        this.tnCheckpoint = TableName.valueOf(this.sysns, TABLE_SYNC_PARAM);
        try {
            this.schemaUtils = new MessageTypeSchemaUtils(sysns);
            OssConfig ossConfig = new OssConfig(
                    antsdbConfig.getOssServiceEndpoint(), 
                    antsdbConfig.getOssRegion(),
                    antsdbConfig.getOssAccessKey(), 
                    antsdbConfig.getOssSecretKey(), 
                    antsdbConfig.getOssBucketName());
            if ( ossConfig.getAccessKey() == null || ossConfig.getSecretKey() == null) {
                _log.debug("aliyun oss auth info cannot be empty or capitalized");
                throw new OrcaObjectStoreException("aliyun oss auther info cannot be empty or capitalized");
            }
            this.provider = new OssStoreProvider(ossConfig);
            
            // check oss config
            if (ossConfig.getClientRegion() == null && ossConfig.getEndpoint() == null) {
                _log.debug("Oss config error, region and endpoint are empty at the same time");
                throw new OrcaObjectStoreException("Oss config error, region and endpoint are empty at the same time");
            }
            if (ossConfig.getBucketName() == null || ossConfig.getBucketName().length() == 0
                    || !ossConfig.getBucketName().equals(ossConfig.getBucketName().toLowerCase())) {
                _log.debug("Bucket name cannot be empty or capitalized");
                throw new OrcaObjectStoreException("Bucket name cannot be empty or capitalized");
            }
            else if (ossConfig.getBucketName().length() < 3 || ossConfig.getBucketName().length() > 63) {
                _log.debug("Bucket Name Length ranges from 3 to 63");
                throw new OrcaObjectStoreException("Bucket Name Length ranges from 3 to 63");
            }
            if (!this.provider.checkRootResourceExists(ossConfig.getBucketName())) {
                _log.debug("Bucket ({}) does not exist, please create!!!", ossConfig.getBucketName() );
                throw new OrcaObjectStoreException(
                        "Bucket (" + ossConfig.getBucketName() + ") does not exist, please create!!!");
            }
            
            initParquetConfig(antsdbConfig);
            
            _log.info("oss store : {}{}", ossConfig.getBucketName(),this.getRemoteDataHome());
 
            boatThreads = new ExecutorBoatPool();
            catchThreads = new ExecutorCatchPool(antsdbConfig.getUplaodThreadCount());
            
            // Initialize oss database for antsdb
            init(); 
            
            dlaEnable = "true".equalsIgnoreCase(antsdbConfig.getDataLakeAnalyticsEnable()) ? true : false;
            _log.debug(" Data Lake Analytics Enable flag is " + dlaEnable);
            if (dlaEnable) {
                this.sourceBucketName = antsdbConfig.getOssBucketName();
                this.dlaUrl = antsdbConfig.getDataLakeUrl();
                this.dlaUser = antsdbConfig.getDataLakeUser();
                this.dlaPasswd = antsdbConfig.getDataLakePasswd();
                if(this.dlaUrl == null || this.dlaUrl.length() ==0
                        ||this.dlaUser == null || this.dlaUser.length() ==0
                        ||this.dlaPasswd == null || this.dlaPasswd.length() ==0
                        ) {
                    throw new OrcaObjectStoreException(
                            "aliyun data lake analytics config error!!!");
                }
            }
               
            this.replicationHandler = new ParquetReplicationHandler(
                    humpback, 
                    this, 
                    this.schemaUtils,
                    this.obsCache,
                    this.isMutable);
        }
        catch (Throwable x) {
            _log.error(x.getMessage(), x);
            throw x;
        }

    }

    @Override
    public void waitForSync(int timeoutSeconds) throws TimeoutException {
        SpaceManager spaceman = this.humpback.getSpaceManager();

        // find out the current space pointer
        long spNow = spaceman.getAllocationPointer();

        // write a bogus rollback so that spNow can be replayed
        HumpbackSession hsession = this.humpback.createSession(":OssStorageService");
        try {
            this.humpback.setConfig(hsession, "waitForSync: {}", UberTime.getTime());
        }
        finally {
            this.humpback.deleteSession(hsession);
        }

        // wait until timeout
        UberTimer timer = new UberTimer(timeoutSeconds * 1000);
        for (;;) {
            if (getCommittedLogPointer() >= spNow) {
                _log.trace("waitForSync :CurrentSP:{},spNow:{}", getCommittedLogPointer(), spNow);
                break;
            }
            if (timer.isExpired()) {
                throw new TimeoutException();
            }
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException ignored) {
            }
        }
    }

    @Override
    public void createDataAnalyticsDatabaseSchema(String databseName) {
        if (dlaEnable) {
            String sql = " CREATE DATABASE IF NOT EXISTS `" + databseName +"`";
            sql += " with DBPROPERTIES( ";
            sql += "  catalog='oss',";
            sql += "  location='oss://" + sourceBucketName + "/" + databseName + "/'";
            sql += " );";
            try (
                    java.sql.Connection conn = getDataAnalyticsConnect();
                    Statement statement = conn.createStatement();
            ) {
                _log.debug("create data analytics schema sql:{}", sql);
                statement.execute("DROP DATABASE IF EXISTS `" + databseName +"` CASCADE");
                statement.execute(sql);
                statement.close();
                conn.close();
            }
            catch (SQLException e) {
                _log.warn(e.getMessage(), e);
                throw new OrcaObjectStoreException("sync data analytics schema error(" + sql + ")", e);
            }
            catch (Exception e) {
                _log.warn(e.getMessage(), e);
                throw new OrcaObjectStoreException("sync data analytics schema error(" + sql + ")", e);
            }
        }
    }

    @Override
    public void dropDataAnalyticsDatabaseSchema(String databseName) {
        if (dlaEnable) {
            String sql = " DROP DATABASE IF EXISTS `" + databseName +"`";
            sql += " CASCADE ";
            try (
                    java.sql.Connection conn = getDataAnalyticsConnect();
                    Statement statement = conn.createStatement();
            ) {
                statement.execute(sql);
                statement.close();
                conn.close();
            }
            catch (SQLException e) {
                _log.warn(e.getMessage(), e);
                throw new OrcaObjectStoreException("sync data analytics schema error(" + sql + ")", e);
            }
            catch (Exception e) {
                _log.warn(e.getMessage(), e);
                throw new OrcaObjectStoreException("sync data analytics schema error(" + sql + ")", e);
            }
        }
    }

    @Override
    public void updateDataAnalyticsTableSchema(TableName tableInfo, List<HColumnRow> columns) {
        if (dlaEnable) {
            if (columns == null || columns.size() == 0) {
                _log.info(tableInfo.getTableName() + "\t mapping columns is empty skip");
                return;
            }
            long startTime = System.currentTimeMillis();
            String sql = " CREATE EXTERNAL TABLE IF NOT EXISTS ";
            sql += tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + " (";
            sql += antsdbTypeToDlaSchena(columns);
            sql += " )";
            sql += " STORED AS PARQUET  ";
            sql += " LOCATION 'oss://" + sourceBucketName + "/" + tableInfo.getTablePath() + "'";
            sql += " TBLPROPERTIES ('has_encrypted_data'='false','parquet.compress'='"+this.compressionType+
                    "','parquet.column.index.access'='false') ";
            try (
                    java.sql.Connection conn = getDataAnalyticsConnect();
                    Statement statement = conn.createStatement();
            ) {
                statement.execute(
                        "DROP TABLE IF EXISTS " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName());
                statement.execute(sql);
                statement.close();
                conn.close();
            }
            catch (SQLException e) {
                _log.warn("sync data analytics schema error(" + sql + ") "+e.getMessage(), e);
            }
            catch (Exception e) {
                _log.warn(e.getMessage(), e);
                throw new OrcaObjectStoreException("sync data analytics schema error(" + sql + ")", e);
            }
            long endTime = System.currentTimeMillis();
            _log.debug("flush data analytics schema use time:{}", (endTime - startTime));
        }
    }

    @Override
    public void dropDataAnalyticsTableSchema(String dbname, String tbname) {
        if (dlaEnable) {
            try (
                    java.sql.Connection conn = getDataAnalyticsConnect();
                    Statement statement = conn.createStatement();
            ) {
                if(tbname.contains("-")) {
                    tbname = "`"+tbname+"`";
                }
                statement.execute("DROP TABLE IF EXISTS " + dbname + "." + tbname);
                statement.close();
                conn.close();
            }
            catch (SQLException e) {
                _log.warn(e.getMessage(), e);
                throw new OrcaObjectStoreException("sync drop data analytics table error(" + tbname + ")", e);
            }
            catch (Exception e) {
                _log.warn(e.getMessage(), e);
                throw new OrcaObjectStoreException("sync drop data analytics table error(" + tbname + ")", e);
            }
        }
    }

    private String antsdbTypeToDlaSchena(List<HColumnRow> columns) {
        StringBuffer sqlBuffer = new StringBuffer();
        List<String> columnsTmp = new ArrayList<>();
        if (columns != null && columns.size() > 0) {

            for (HColumnRow columnObj : columns) {
                if (columnObj == null) {
                    continue;
                }
                String columnName = columnObj.getColumnName();
                if (columnName.startsWith("*") || columnObj.isDeleted()) {
                    continue;
                }
                columnName = "`" + columnName +"`";
                
                if (sqlBuffer.length() > 0) {
                    sqlBuffer.append(",");
                }
                int anstdbType = columnObj.getType();
                String dlaType = antsdbType2DlaType(anstdbType);
                _log.trace("mapping to athena :columnName:{},anstdbType:{},athenaType:{}", 
                        columnName, 
                        anstdbType,
                        dlaType);
                columnsTmp.add(columnName);
                sqlBuffer.append(columnName + " " + dlaType);
            }
        }

        return sqlBuffer.toString();
    }

    private static String antsdbType2DlaType(int ansdbType) {
        String dlaType = "";
        switch (ansdbType) {
            case Value.TYPE_NULL:
                break;
            case Value.TYPE_NUMBER:
            case Value.FORMAT_INT4:
                dlaType = "BIGINT";
                break;
            case Value.FORMAT_INT8:
                dlaType = "BIGINT";
                break;
            case Value.FORMAT_BIGINT:
                dlaType = "DECIMAL(38)";
                break;
            case Value.FORMAT_FAST_DECIMAL:
            case Value.FORMAT_DECIMAL:
                dlaType = "DECIMAL(38,5)";
                break;
            case Value.TYPE_STRING:
            case Value.FORMAT_UTF8:
            case Value.FORMAT_UNICODE16:
                dlaType = "VARCHAR(65532)";
                break;
            case Value.TYPE_BOOL:
                dlaType = "BOOLEAN";
                break;
            case Value.TYPE_DATE:
                dlaType = "BIGINT";
                break;
            case Value.TYPE_TIME:
            case Value.TYPE_TIMESTAMP:
                dlaType = "TIMESTAMP";
                break;
            case Value.FORMAT_FLOAT4:
                dlaType = "FLOAT";
                break;
            case Value.FORMAT_FLOAT8:
                dlaType = "DOUBLE";
                break;
            case Value.TYPE_BYTES:
            case Value.TYPE_CLOB:
            case Value.TYPE_BLOB:
            case Value.TYPE_UNKNOWN:
            case Value.FORMAT_RECORD:
            case Value.FORMAT_KEY_BYTES:
            case Value.FORMAT_INT4_ARRAY:
            case Value.FORMAT_CLOB_REF:
            case Value.FORMAT_BLOB_REF:
            case Value.FORMAT_ROW:
                dlaType = "BINARY";
                break;
        }
        return dlaType;
    }

    private Connection getDataAnalyticsConnect() throws Exception {
        Connection connection=(Connection)DriverManager.getConnection(dlaUrl,dlaUser,dlaPasswd);
        return connection;
    }

}
