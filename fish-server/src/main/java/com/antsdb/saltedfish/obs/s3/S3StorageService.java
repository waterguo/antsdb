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
package com.antsdb.saltedfish.obs.s3;

import java.io.File;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
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
import com.simba.athena.amazonaws.regions.Region;
import com.simba.athena.amazonaws.regions.RegionUtils;
import com.simba.athena.amazonaws.services.athena.AmazonAthena;

/**
 *
 * @author Frank Li<lizc@tg-hd.com>
 */
public class S3StorageService extends BaseStorageService {
    static Logger _log = UberUtil.getThisLogger();

    public final static String athenaOutputLocationFormat = "s3://%s/@--athenaresult/";
    
    private boolean atheanEnable = false;
    private String atheanS3OutputLocation;
    private String atheanJdbcUrl = "";
    private String accessKey;
    private String secretKey;
    private String sourceBucketName;
    
    private String endpointOverride;

    public S3StorageService(Humpback humpback) throws Exception {
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
        _log.info("system namespace: {}", this.sysns);
        this.tnCheckpoint = TableName.valueOf(this.sysns, TABLE_SYNC_PARAM);
        try {
            this.schemaUtils = new MessageTypeSchemaUtils(sysns);
            S3Config s3config = new S3Config(
                    antsdbConfig.getS3ServiceEndpoint(), 
                    antsdbConfig.getS3Region(),
                    antsdbConfig.getS3accessKey(), 
                    antsdbConfig.getS3secretKey(), 
                    antsdbConfig.getS3BucketName());
            if (s3config.getClientRegion() == null || s3config.getClientRegion().length() == 0
                    || s3config.getAccessKey() == null || s3config.getSecretKey() == null) {
                _log.debug("s3 auth info cannot be empty or capitalized");
                throw new OrcaObjectStoreException("s3 auther info cannot be empty or capitalized");
            }
            this.provider = new S3StoreProvider(s3config);
            
            // check s3 config
            if (s3config.getBucketName() == null || s3config.getBucketName().length() == 0
                    || !s3config.getBucketName().equals(s3config.getBucketName().toLowerCase())) {
                _log.debug("Bucket name cannot be empty or capitalized");
                throw new OrcaObjectStoreException("Bucket name cannot be empty or capitalized");
            }
            else if (s3config.getBucketName().length() < 3 || s3config.getBucketName().length() > 63) {
                _log.debug("Bucket Name Length ranges from 3 to 63");
                throw new OrcaObjectStoreException("Bucket Name Length ranges from 3 to 63");
            }
            if (!this.provider.checkRootResourceExists(s3config.getBucketName())) {
                _log.debug("Bucket (" + s3config.getBucketName() + ") does not exist, please create!!!");
                throw new OrcaObjectStoreException(
                        "Bucket (" + s3config.getBucketName() + ") does not exist, please create!!!");
            }
            
            initParquetConfig(antsdbConfig);
            
            _log.info("s3 store: {}{}", s3config.getBucketName(),this.getRemoteDataHome());
 
            boatThreads = new ExecutorBoatPool();
            catchThreads = new ExecutorCatchPool(antsdbConfig.getUplaodThreadCount());
            
            // Initialize s3 database for antsdb
            init();
            
            atheanEnable = "true".equalsIgnoreCase(antsdbConfig.getAthenaEnable()) ? true : false;
            _log.debug("athena use flag is " + atheanEnable);
            if (atheanEnable) {
                atheanS3OutputLocation = String.format(athenaOutputLocationFormat, s3config.getBucketName());
                atheanJdbcUrl = "jdbc:awsathena://AwsRegion=" + antsdbConfig.getS3Region() + ";";
                
/*                atheanJdbcUrl="jdbc:awsathena://athena."+antsdbConfig.getAthenaRegion()+".amazonaws.com.cn:443;";
                atheanJdbcUrl += "AwsRegion=" + antsdbConfig.getS3Region() + ";";
                atheanJdbcUrl += "User=" + antsdbConfig.getS3accessKey() + ";";
                atheanJdbcUrl += "Password=" + antsdbConfig.getS3secretKey() + ";";
                atheanJdbcUrl += "S3OutputLocation="+ atheanS3OutputLocation + ";";
                */
                
                accessKey = s3config.getAccessKey();
                secretKey = s3config.getSecretKey();
                sourceBucketName = antsdbConfig.getS3BucketName();
                endpointOverride = antsdbConfig.getAthenaEndpointOverride();
                Region region = RegionUtils.getRegion(antsdbConfig.getS3Region()/*"CN_NORTHWEST_1"*/);
                String athenaEndpoint = region.getServiceEndpoint(AmazonAthena.ENDPOINT_PREFIX);
                _log.debug("athena endpoint:{}",athenaEndpoint);
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
        HumpbackSession hsession = this.humpback.createSession(":S3StorageService");
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
        if (atheanEnable) {
            String sql = " CREATE DATABASE IF NOT EXISTS `" + databseName +"`";
            sql += " COMMENT '" + databseName + "'";
            sql += "  LOCATION 's3://" + sourceBucketName + "/" + databseName + "/'";
            try (
                    java.sql.Connection conn = getAtheanConnect();
                    Statement statement = conn.createStatement();
            ) {
                _log.debug("create athena schema sql:{}", sql);
                statement.execute("DROP DATABASE IF EXISTS `" + databseName +"` CASCADE");
                statement.execute(sql);
                statement.close();
                conn.close();
            }
            catch (SQLException e) {
                _log.warn(e.getMessage(), e);
                throw new OrcaObjectStoreException("sync athena schema error(" + sql + ")", e);
            }
            catch (Exception e) {
                _log.warn(e.getMessage(), e);
                throw new OrcaObjectStoreException("sync athena schema error(" + sql + ")", e);
            }
        }
    }

    @Override
    public void dropDataAnalyticsDatabaseSchema(String databseName) {
        if (atheanEnable) {
            String sql = " DROP DATABASE IF EXISTS `" + databseName+"`";
            sql += " CASCADE ";
            try (
                    java.sql.Connection conn = getAtheanConnect();
                    Statement statement = conn.createStatement();
            ) {
                statement.execute(sql);
                statement.close();
                conn.close();
            }
            catch (SQLException e) {
                _log.warn(e.getMessage(), e);
                throw new OrcaObjectStoreException("sync athena schema error(" + sql + ")", e);
            }
            catch (Exception e) {
                _log.warn(e.getMessage(), e);
                throw new OrcaObjectStoreException("sync athena schema error(" + sql + ")", e);
            }
        }
    }

    @Override
    public void updateDataAnalyticsTableSchema(TableName tableInfo, List<HColumnRow> columns) {
        if (atheanEnable) {
            if (columns == null || columns.size() == 0) {
                _log.info(tableInfo.getTableName() + "\t mapping columns is empty skip");
                return;
            }
            long startTime = System.currentTimeMillis();
            String sql = " CREATE EXTERNAL TABLE IF NOT EXISTS ";
            sql += tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + " (";
            sql += antsdbTypeToAthenaSchena(columns);
            sql += " )";
            sql += " STORED AS PARQUET  ";
            sql += " LOCATION 's3://" + sourceBucketName + "/" + tableInfo.getTablePath() + "'";
            sql += " TBLPROPERTIES ('has_encrypted_data'='false','parquet.compress'='"+this.compressionType+"','parquet.column.index.access'='false') ";
            try (
                    java.sql.Connection conn = getAtheanConnect();
                    Statement statement = conn.createStatement();
            ) {
                statement.execute(
                        "DROP TABLE IF EXISTS " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName());
                _log.debug("athena sql:{}",sql);
                statement.execute(sql);
                statement.close();
                conn.close();
            }
            catch (SQLException e) {
                _log.warn("sync athena schema error(" + sql + ") "+e.getMessage(), e);
            }
            catch (Exception e) {
                _log.warn(e.getMessage(), e);
                throw new OrcaObjectStoreException("sync athena schema error(" + sql + ")", e);
            }
            long endTime = System.currentTimeMillis();
            _log.debug("flush athena schema use time:{}", (endTime - startTime));
        }
    }

    @Override
    public void dropDataAnalyticsTableSchema(String dbname, String tbname) {
        if (atheanEnable) {
            try (
                    java.sql.Connection conn = getAtheanConnect();
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
                throw new OrcaObjectStoreException("sync drop athena table error(" + tbname + ")", e);
            }
            catch (Exception e) {
                _log.warn(e.getMessage(), e);
                throw new OrcaObjectStoreException("sync drop athena table error(" + tbname + ")", e);
            }
        }
    }

    private String antsdbTypeToAthenaSchena(List<HColumnRow> columns) {
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
                String athenaType = antsdbType2AthenaType(anstdbType);
                _log.debug("mapping to athena :columnName:{},anstdbType:{},athenaType:{}", 
                        columnName, 
                        anstdbType,
                        athenaType);
                columnsTmp.add(columnName);
                sqlBuffer.append(columnName + " " + athenaType);
            }
        }

        return sqlBuffer.toString();
    }

    private static String antsdbType2AthenaType(int ansdbType) {
        String athenaType = "";
        switch (ansdbType) {
            case Value.TYPE_NULL:
                break;
            case Value.TYPE_NUMBER:
            case Value.FORMAT_INT4:
                athenaType = "BIGINT";
                break;
            case Value.FORMAT_INT8:
                athenaType = "BIGINT";
                break;
            case Value.FORMAT_BIGINT:
                athenaType = "DECIMAL(38)";//
                break;
            case Value.FORMAT_FAST_DECIMAL:
            case Value.FORMAT_DECIMAL:
                athenaType = "DECIMAL(38,5)";
                break;
            case Value.TYPE_STRING:
            case Value.FORMAT_UTF8:
            case Value.FORMAT_UNICODE16:
                athenaType = "VARCHAR(65532)";
                break;
            case Value.TYPE_BOOL:
                athenaType = "BOOLEAN";
                break;
            case Value.TYPE_DATE:
                // athenaType = "DATE";//athena query date is wrong
                athenaType = "BIGINT";
                break;
            case Value.TYPE_TIME:
            case Value.TYPE_TIMESTAMP:
                athenaType = "TIMESTAMP";
                break;
            case Value.FORMAT_FLOAT4:
                athenaType = "FLOAT";
                break;
            case Value.FORMAT_FLOAT8:
                athenaType = "DOUBLE";
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
                athenaType = "BINARY";
                break;
        }
        return athenaType;
    }

    private java.sql.Connection getAtheanConnect() throws Exception {
        Properties info = new Properties();
        info.put("User", accessKey);
        info.put("Password", secretKey);
        info.put("S3OutputLocation", atheanS3OutputLocation);
        if(endpointOverride!=null && endpointOverride.length() > 0) {
            info.put("EndpointOverride", endpointOverride);
        }
        Class.forName("com.simba.athena.jdbc.Driver");
        java.sql.Connection connection = DriverManager.getConnection(atheanJdbcUrl, info);
        return connection;
    }

}
