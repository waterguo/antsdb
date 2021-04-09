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
package com.antsdb.saltedfish.obs;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.function.Consumer;

import org.apache.commons.codec.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;

import com.antsdb.mysql.network.PacketUtil;
import com.antsdb.saltedfish.charset.Decoder;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.nosql.SlowRow;
import com.antsdb.saltedfish.nosql.StorageEngine;
import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.obs.s3.S3StorageService;
import com.antsdb.saltedfish.parquet.ObsService;
import com.antsdb.saltedfish.parquet.OrcaObjectStoreException;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.SystemParameters;
import com.antsdb.saltedfish.sql.vdm.AsynchronousInsert;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.util.UberUtil;
import com.antsdb.spark.launcher.FishLauncher;

public final class ExternalQuerySession implements AutoCloseable {
    static Logger _log = UberUtil.getThisLogger();
    AsynchronousInsert asyncExecutor;
    private SystemParameters config;
    private com.antsdb.saltedfish.nosql.ConfigService antsdbConfig = null;

    private ResultSet result = null;

    private java.sql.Connection conn = null;
    private Statement statement = null;
    private Configuration hdfsConfig = null;
    private File home;

    private ExternalQuerySession() {
        this.config = new SystemParameters();
    }

    public void init(File home, com.antsdb.saltedfish.nosql.ConfigService config) {
        this.home = home;
        antsdbConfig = config;
    }

    public static ExternalQuerySession getInstance() {
        return SingleHolder.INSTANCE;
    }

    private static class SingleHolder {
        private static final ExternalQuerySession INSTANCE = new ExternalQuerySession();
    }

    public Object run(String sessionId, Humpback humpback, String sql, Parameters params, Consumer<Object> callback)
            throws Exception {
        byte[] bytes = sql.getBytes(Charsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocateDirect(bytes.length);
        buf.put(bytes);
        buf.flip();
        return run(sessionId, humpback, buf, params, callback);
    }

    public boolean athenaEnable() {
        return "true".equalsIgnoreCase(antsdbConfig.getAthenaEnable()) ? true : false;
    }

    public boolean sparksqlEnable() {
        return "true".equalsIgnoreCase(antsdbConfig.getSparksqlEnable()) ? true : false;
    }
    public boolean dlaEnable() {
        return "true".equalsIgnoreCase(antsdbConfig.getDataLakeAnalyticsEnable()) ? true : false;
    }

    public Object run(String sessionId, Humpback humpback, ByteBuffer cbuf, Parameters params,
            Consumer<Object> callback) throws SQLException, ClassNotFoundException {
        CharBuffer sql = toCharBuffer(cbuf);
        Object result = null;
        String sqlStr = sql.subSequence(1, sql.length()).toString();
        if (sqlStr.toLowerCase().startsWith("select")) {
            if (athenaEnable()) {
                try {
                    conn = getAtheanConnect();
                    statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    statement.setFetchSize(1000);
                    _log.trace("exec query by athena :{}", sqlStr);
                    result = statement.executeQuery(sqlStr);
                    if (callback != null) {
                        callback.accept(result);
                    }
                    return result;
                }
                catch (SQLException e) {
                    _log.warn(e.getMessage(), e);
                    throw new OrcaObjectStoreException("athena query sql:({}) error:{}",  sql,e);
                }
                catch (Exception e) {
                    _log.warn(e.getMessage(), e);
                    throw new OrcaObjectStoreException("athena query sql:({}) error:{}",  sql,e);
                }
            }
            else if (sparksqlEnable()) {
                if(sqlStr.endsWith(";")) {// del end ;
                    sqlStr =  sqlStr.substring(0,sqlStr.length()-1);
                }
                _log.trace("spark-sql query by sql:{}", sqlStr);
                try {
                    String sparkHome = antsdbConfig.getSparkHome();
                    String master = antsdbConfig.getSparkMaster();
                    String deployMode = antsdbConfig.getSparkMode();
                    String sparkTaskJar = antsdbConfig.getSparkTaskJar();
                    if (!sparkTaskJar.startsWith("hdfs:/") && !sparkTaskJar.startsWith("file:/")
                            && sparkTaskJar.indexOf(":") != 1) {
                        String os = System.getProperty("os.name");
                        String pathPro = "";
                        if (os != null && os.toLowerCase().contains("windows")) {
                            pathPro = "file:/";
                        }
                        else {
                            pathPro = "file://";
                        }
                        File sparkTaskJarFile = new File(home, sparkTaskJar);
                        sparkTaskJar = pathPro + sparkTaskJarFile.getAbsolutePath();
                    }
                    StorageEngine stor = humpback.getStorageEngine0();
                    ObsService service = null;
                    if (stor instanceof ObsService) {
                        service = (ObsService) stor;
                    }
                    else {
                        throw new OrcaObjectStoreException("obs service instance error");
                    }
                    
                    String datahome = service.getRemoteDataHome();
                    this.hdfsConfig = service.getHdfsConfiguration();
                    String tableNamePaths = "";
                    String tableNames = "";
                    // select table_id,NAMESPACE from antsdb.x0 where DELETE_MARK=0 and TABLE_NAME = 'usertable';
                    GTable gtable = humpback.getTable(Humpback.SYSMETA_TABLE_ID);
                    for (RowIterator i = gtable.scan(0, Long.MAX_VALUE, true); i.next();) {
                        SysMetaRow meta = new SysMetaRow(SlowRow.from(i.getRow()));
                        if (!meta.isDeleted() && meta.getType() == TableType.DATA && meta.getTableId() > 100) {
                            int tableId = meta.getTableId();
                            String dbname = meta.getNamespace();
                            String tableName = meta.getTableName();
                            if (tableNamePaths.length() > 0) {
                                tableNamePaths += ",";
                            }
                            if (tableNames.length() > 0) {
                                tableNames += ",";
                            }
                            tableNamePaths += dbname + "/" + tableName + "-" + tableId + "/";
                            tableNames += tableName;
                        }
                    }
                    // List<HColumnRow> columns = humpback.getColumns(tableId);
                    String parquetDataRootPath = datahome;
                    String resultTmpPath = antsdbConfig.getSparkResultPath() + "/" + sessionId;
                    if (!resultTmpPath.startsWith("hdfs:/") && !resultTmpPath.startsWith("file:/")
                            && resultTmpPath.indexOf(":") != 1) {
                        resultTmpPath = datahome + resultTmpPath;
                    }
                    FishLauncher launcher = new FishLauncher();
                    String[] args = new String[12];
                    args[0] = sparkHome;
                    args[1] = master;
                    args[2] = deployMode;
                    args[3] = sparkTaskJar;
                    args[4] = "com.antsdb.spark.exec.SparkSQLMultifileParquetFileOps";
                    args[5] = parquetDataRootPath;
                    args[6] = sqlStr;
                    args[7] = tableNamePaths;
                    args[8] = tableNames;
                    args[9] = resultTmpPath;
                    args[10] = antsdbConfig.getHadoopConfigDir();
                    args[11] = antsdbConfig.getHdfsUser();
                    String resultPath = launcher.execSparkQuery(args);
                    result = resultPath;
                    if (callback != null) {
                        callback.accept(result);
                    }
                    return result;
                }
                catch (OrcaException e){
                    _log.warn(e.getMessage(), e);
                    throw new OrcaObjectStoreException("spark sql query error by ({}) result not find,please go to "
                            + "yarn or spark cluster application tracking page check application log! error:{}", sql,e)  ;
                }
                catch (Exception e) {
                    _log.warn(e.getMessage(), e);
                    throw new OrcaObjectStoreException("spark sql query sql:({}) error:{}",sql,e)  ;
                }
            }
            else if(dlaEnable()) {
                try {
                    conn = getDlaConnect();
                    statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    statement.setFetchSize(Integer.MIN_VALUE);
                    result = statement.executeQuery(sqlStr);
                    if (callback != null) {
                        callback.accept(result);
                    }
                    return result;
                }
                catch (SQLException e) {
                    _log.warn(e.getMessage(), e);
                    throw new OrcaObjectStoreException("Data Lake Analytics query sql:({}) error:{},",  sql,e)  ;
                }
                catch (Exception e) {
                    _log.warn(e.getMessage(), e);
                    throw new OrcaObjectStoreException("Data Lake Analytics query sql:({}) error:{},",  sql,e)  ;
                }
            }
            else {
                throw new OrcaObjectStoreException("external sql query engine error");
            }
        }
        else {
            throw new OrcaObjectStoreException("external query error : {}", sql);
        }
    }

    private CharBuffer toCharBuffer(ByteBuffer buf) {
        long pSql = UberUtil.getAddress(buf);
        Decoder decoder = config.getRequestDecoder();
        CharBuffer result = PacketUtil.readStringAsCharBufWithMysqlExtension(pSql, buf.limit(), decoder);
        result.flip();
        return result;
    }

    @Override
    public void close() throws SQLException {
        if (result != null) {
            result.close();
        }
        if (statement != null) {
            statement.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    private java.sql.Connection getAtheanConnect() throws Exception {
        String atheanJdbcUrl = "jdbc:awsathena://AwsRegion=" + antsdbConfig.getS3Region() + ";";
        Properties info = new Properties();
        info.put("User", antsdbConfig.getS3accessKey());
        info.put("Password", antsdbConfig.getS3secretKey());
        info.put("S3OutputLocation", String.format(S3StorageService.athenaOutputLocationFormat, antsdbConfig.getS3BucketName()));
        String endpointOverride = antsdbConfig.getAthenaEndpointOverride();
        if(endpointOverride!=null && endpointOverride.length() > 0) {
            info.put("EndpointOverride", endpointOverride);
        }
        Class.forName("com.simba.athena.jdbc.Driver");
        java.sql.Connection connection = DriverManager.getConnection(atheanJdbcUrl, info);
        return connection;
    }
    
    private java.sql.Connection getDlaConnect() throws Exception {
        String dlaUrl = antsdbConfig.getDataLakeUrl();
        String dlaUser = antsdbConfig.getDataLakeUser();
        String dlaPasswd = antsdbConfig.getDataLakePasswd();
        
        Connection connection=(Connection)DriverManager.getConnection(dlaUrl,dlaUser,dlaPasswd);
        return connection;
    }

    public Configuration getHdfsConfig() {
        return hdfsConfig;
    }

}
