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
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.example.data.Group;

import com.antsdb.saltedfish.nosql.ConfigService;
import com.antsdb.saltedfish.obs.aliyun.OssConfig;
import com.antsdb.saltedfish.obs.aliyun.OssStoreProvider;
import com.antsdb.saltedfish.obs.hdfs.HdfsProvider;
import com.antsdb.saltedfish.obs.s3.S3Config;
import com.antsdb.saltedfish.obs.s3.S3StoreProvider;
import com.antsdb.saltedfish.parquet.Helper;
import com.antsdb.saltedfish.parquet.OrcaObjectStoreException;
import com.antsdb.saltedfish.parquet.ParquetDataReader;
import com.antsdb.saltedfish.parquet.ParquetUtils;
import com.antsdb.saltedfish.parquet.bean.Partition;
import com.antsdb.saltedfish.parquet.bean.PartitionIndexWarp;
import com.antsdb.saltedfish.slave.DbUtils;
import com.antsdb.saltedfish.sql.FishCommandLine;
import com.antsdb.saltedfish.util.MysqlJdbcUtil;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 对比antsdb中的数据与obs中的数据数是否一致
 * @author lizc@tg-hd.com
 */
public class ObsDataDiffMain extends FishCommandLine {

    private ConfigService antsdbConfig;

    private File home;
    private String remoteDataHome;
    private File localDataHome;
    private ObsProvider provider = null;
    Configuration configuration = new Configuration();
    
    private File temp;
    
    private String host;
    private String port; 
    private String user;
    private String password;
    private String function;
    

    public ObsDataDiffMain(String[] args) throws ParseException {
        super(args);
    }

    public static void main(String[] args) throws Exception {
        new ObsDataDiffMain(args).run();
        System.out.println("Obs data diff is completed");
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption("h", "help", false, "print help");
        options.addOption(null, "home", true, "antsdb home");
        options.addOption(null, "temp", true, "show data temp home");
        options.addOption(null, "fun", true, " show file name or data content");
        options.addOption(null, "dbname", true, "antsdb database name");
        options.addOption(null, "tbname", true, "antsdb table name");
        options.addOption(null, "host", true, "host");
        options.addOption(null, "port", true, "port");
        options.addOption(null, "user", true, "user");
        options.addOption(null, "password", true, "password");
        return options;
    }

    public String getDbName() {
        String dbname = cmd.getOptionValue("dbname");
        return dbname;
    }

    public String getTempHome() {
        String temp = cmd.getOptionValue("temp");
        return temp;
    }
    
    public String getTbName() {
        String tbname = cmd.getOptionValue("tbname");
        return tbname;
    }

    @Override
    protected String getName() {
        return null;
    }

    @Override
    protected String getCommandName() {
        return "antsdb-Obs-diff diff antsdb and obs data";
    }

    private void run() throws Exception {
        home = getHome();
        File configFile = new File(getHome(), "conf/conf.properties");
        if (!configFile.exists()) {
            println("error: config file is not found: %s", configFile);
            System.exit(-1);
        }
        // connecting
        println("AntsDB home: %s", home);
        this.antsdbConfig = new ConfigService(configFile);

        String localDir = antsdbConfig.getLocalData();
        this.localDataHome = new File(home, localDir);

        initProvider();

        println("obs provider inited");
        println("");
        if (getTempHome() == null || getTempHome().length() == 0) {
            println("temp home  is empty,please input temp value");
            return;
        }
        temp = new File(this.getTempHome());
        if(!temp.exists() || !temp.isDirectory()) {
            println("temp home  is not exists or is not directory,please input temp value");
            return;
        }
        
        String sql =  "select * from antsdb.x0 where DELETE_MARK=0";
        if(this.getDbName() !=null && this.getDbName().length() > 0 ) {
            sql += "and NAMESPACE = '";
            sql += this.getDbName() +"'";
        }
        
        if(this.getTbName() !=null && this.getTbName().length() > 0 ) {
            sql += "and TABLE_NAME = '";
            sql += this.getTbName() +"'";
        }
        
        this.host = this.cmd.getOptionValue("host", "localhost");
        this.port = this.cmd.getOptionValue("port", "3306");
        this.user = this.cmd.getOptionValue("user", "");
        this.password = this.cmd.getOptionValue("password", "");
        
        this.function = cmd.getOptionValue("fun");
        
        Connection conn = this.createConnection();
        
        List<Map<String, Object>> rows = DbUtils.rows(conn, sql);
        
        if(rows == null || rows.size() ==0) {
             println("%s is empty",sql);
             return;
        }
        
        for(Map<String, Object> row :rows) {
            int tableId = (int) row.get("TABLE_ID");
            String dbname = (String) row.get("NAMESPACE");
            String tbname = (String) row.get("TABLE_NAME");
            String type = (String) row.get("TABLE_TYPE");
            String srcSql = "";
            try {
                long srcCount = -1;
                
                if(type.equalsIgnoreCase("DATA") && !tbname.endsWith("_blob_")) {
                    srcSql = "select count(*) count from "+dbname +"."+tbname;
                    Map<String, Object> srcResult = DbUtils.firstRow(conn, srcSql);
                    srcCount = (Long) srcResult.get("count");
                }
                else {
                    srcSql = ".hselect * from "+tableId;
                    List<Map<String, Object>> srcResult = DbUtils.rows(conn, srcSql);
                    srcCount = srcResult.size();
                }
                long destCount = getObsDataCount(dbname, tbname, tableId);
                boolean result = (srcCount == destCount)?true:false;
                if(!result) {
                    println("please check db=%s type=%s tb=%s tId=%s src=%s obs=%s result=%s",
                            dbname, 
                            type,
                            tbname,
                            tableId,
                            srcCount,
                            destCount,
                            result);
                }
                if("detail".equalsIgnoreCase(function)) {
                    ConcurrentSkipListMap<String,byte[]> obsdatas = new ConcurrentSkipListMap<>(
                            new Comparator<String>(){
                                @Override
                                public int compare(String o1, String o2) {
                                    return o1.compareTo(o2);
                                }
                            } 
                        );
                    if(type.equalsIgnoreCase("INDEX")) {
                        List<Map<String, Object>> srcResult = DbUtils.rows(conn, srcSql);
                        if(srcResult == null) {
                            continue;
                        }
                        getObsDatas(obsdatas,dbname, tbname, tableId);
                        
                        for(Map<String, Object> data : srcResult) {
                            String indexKey = (String)data.get("00");//index key
                            String rowKey = (String)data.get("0");//row key
                            byte[] obsRowKey =  obsdatas.get(indexKey.replace("-", ""));
                            if(obsRowKey == null) {
                                println("<@ rowKey=%s indexKey=%s",rowKey,indexKey);
                            }
                        }
                    }
                }
            }catch(Exception e) {
                println(srcSql);
                e.printStackTrace();
            }
        }
            
        this.provider.close();
        this.provider = null;
        println("done");
        println("Obs data diff run is completed");
    }
    
    private long getObsDataCount(String dbname, String tbname, int tableId) throws Exception {
        String databasePath = dbname;
        String prefix = String.format("%s-%08x", tbname,tableId);
        List<String> tableIndexFiles = this.provider.listFiles(databasePath, prefix,
                ParquetUtils.DATA_JSON_EXT_NAME);
        long dataCount = 0;
        if (tableIndexFiles != null && tableIndexFiles.size() > 0 ) {
            for (String tableIndex : tableIndexFiles) {
                if (tableIndex.endsWith("SYNCPARAM.json")) {
                    continue;
                }
                String tmpTableIndex = tableIndex.substring(tableIndex.lastIndexOf("/") + 1);
                 
                String partitionIndexFileName = tmpTableIndex;
                dataCount += getTableDataRowCount(dbname, partitionIndexFileName);
            }
        }
        return dataCount;
    }
    
    private void getObsDatas(ConcurrentSkipListMap<String,byte[]> obsdatas,
            String dbname, 
            String tbname, 
            int tableId) throws Exception {
        String databasePath = dbname;
        String prefix = String.format("%s-%08x", tbname,tableId);
        List<String> tableIndexFiles = this.provider.listFiles(databasePath, prefix,
                ParquetUtils.DATA_JSON_EXT_NAME);
        if (tableIndexFiles != null && tableIndexFiles.size() > 0 ) {
            for (String tableIndex : tableIndexFiles) {
                if (tableIndex.endsWith("SYNCPARAM.json")) {
                    continue;
                }
                String tmpTableIndex = tableIndex.substring(tableIndex.lastIndexOf("/") + 1);
                 
                String partitionIndexFileName = tmpTableIndex;
                
                String objectKey = dbname + "/" + partitionIndexFileName;
                
                File filename = new File(temp , objectKey);
                this.provider.downloadObject(objectKey, filename.getAbsolutePath());
                PartitionIndexWarp datas = new PartitionIndexWarp();
                try {
                    String contents = FileUtils.readFileToString(filename);
                    datas = UberUtil.toObject(contents, PartitionIndexWarp.class);
                }
                catch (Exception e) {
                    throw new OrcaObjectStoreException(e);
                }
                finally {
                    FileUtils.forceDelete(filename);
                }
                if (datas != null && datas.getPartitions() != null && datas.getPartitions().size() > 0) {

                    for (Partition partition : datas.getPartitions()) {
                        String dataObjectKey = dbname + "/" + partition.getTableName() + "-" 
                                + partition.getTableId() + "/" 
                                + partition.getVersionFileName();
                        showDataFileContent(obsdatas,dataObjectKey);
                    }
                }
            }
        }
    }
    
    private long showDataFileContent(ConcurrentSkipListMap<String,byte[]> obsdatas, String dataFile) throws Exception {
        long count = 0;
        if(provider.doesObjectExist(dataFile)) {
            File filePath = new File(temp , dataFile);
            provider.downloadObject(dataFile, filePath.getAbsolutePath());
            
            try (ParquetDataReader readers = new ParquetDataReader(new Path(filePath.getAbsolutePath()), configuration)) {
                Group group = null;
                while ((group = readers.readNext()) != null) {
                    count ++;
                    byte[] rowkey = null;// row key
                    byte[] indexKey = null;//index key
                    int rowKeyCount = group.getFieldRepetitionCount(Helper.SYS_COLUMN_PARQUETKEY_BYTES);
                    if(rowKeyCount >0) {
                        rowkey = group.getBinary(Helper.SYS_COLUMN_PARQUETKEY_BYTES, 0).getBytes();
                    }
                    int keyCount = group.getFieldRepetitionCount(Helper.SYS_COLUMN_INDEXKEY_BYTES);
                    if(keyCount >0) {
                        indexKey = group.getBinary(Helper.SYS_COLUMN_INDEXKEY_BYTES, 0).getBytes();
                    }
                    String strKey = bytesToHexString(indexKey);
                    obsdatas.put(strKey, rowkey);
                }
            }
            catch (Exception e) {
                throw new OrcaObjectStoreException(e);
            }finally {
                FileUtils.forceDelete(filePath);
            }
        }
        else {
            println("data file %s not exist",dataFile);
        }
        return count;
    }
    
    private static String bytesToHexString(byte[] src) {
        StringBuilder stringBuilder = new StringBuilder("");
        if (src == null || src.length <= 0) {
            return null;
        }
        for (int i = 0; i < src.length; i++) {
            int v = src[i] & 0xFF;
            String hv = Integer.toHexString(v);
            if (hv.length() < 2) {
                stringBuilder.append(0);
            }
            stringBuilder.append(hv);
        }
        return stringBuilder.toString();
    }
    
    private long getTableDataRowCount(String dbname, String partitionIndexFileName) throws Exception {
        String objectKey = dbname + "/" + partitionIndexFileName;
        long rows = 0;
        File filename = new File(temp , objectKey);
        this.provider.downloadObject(objectKey, filename.getAbsolutePath());
        PartitionIndexWarp datas = new PartitionIndexWarp();
        try {
            String contents = FileUtils.readFileToString(filename);
            datas = UberUtil.toObject(contents, PartitionIndexWarp.class);
        }
        catch (Exception e) {
            throw new OrcaObjectStoreException(e);
        }
        finally {
            FileUtils.forceDelete(filename);
        }
        if (datas != null && datas.getPartitions() != null && datas.getPartitions().size() > 0) {

            for (Partition partition : datas.getPartitions()) {
                rows += partition.getRowCount();
            }
        }
        return rows;
    }

    private Connection createConnection() throws SQLException {
        String url = MysqlJdbcUtil.getUrl(this.host, Integer.parseInt(this.port), null);
        Properties props = new Properties();
        props.setProperty("user", this.user==null ? "" : this.user);
        props.setProperty("password", this.password==null ? "" : this.password);
        props.setProperty("useServerPrepStmts", "true");
        Connection conn = DriverManager.getConnection(url, props);
        DbUtils.execute(conn, "SET FOREIGN_KEY_CHECKS=0");
        DbUtils.execute(conn, "SET UNIQUE_CHECKS=0");
        DbUtils.execute(conn, "SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO'");
        return conn;
    }
    

    private void initProvider() throws IOException {

        String strorageEngine = antsdbConfig.getStorageEngineName();

        String compressCodec = antsdbConfig.getDatafileCompressionCodec();
        if (compressCodec == null || compressCodec.length() == 0) {
            compressCodec = "UNCOMPRESSED";
        }
        if ("hdfs".equalsIgnoreCase(strorageEngine)) {

            String uripath = antsdbConfig.getHdfsUri();
            if (uripath != null && uripath.indexOf(":/") <= 0) {
                String os = System.getProperty("os.name");
                if (os.toLowerCase().startsWith("win")) {
                    String tmpPath = new File(uripath).getAbsolutePath().replace("\\", "/");
                    tmpPath = tmpPath.substring(tmpPath.indexOf(":/") + 2);
                    uripath = "file:/" + tmpPath + "/";
                }
                else {
                    uripath = "file://" + new File(uripath).getAbsolutePath();
                }
            }
            if (uripath != null && !uripath.endsWith("/")) {
                uripath += "/";
            }

            if (uripath != null && !uripath.endsWith("/")) {
                uripath += "/";
            }
            this.remoteDataHome = uripath;

            Configuration hdfsConf = getHdfsConfig(antsdbConfig);
            provider = new HdfsProvider(hdfsConf, uripath);
        }
        else if ("s3".equalsIgnoreCase(strorageEngine)) {
            S3Config s3config = new S3Config(antsdbConfig.getS3ServiceEndpoint(), antsdbConfig.getS3Region(),
                    antsdbConfig.getS3accessKey(), antsdbConfig.getS3secretKey(), antsdbConfig.getS3BucketName());
            if (s3config.getClientRegion() == null || s3config.getClientRegion().length() == 0
                    || s3config.getAccessKey() == null || s3config.getSecretKey() == null) {
                throw new OrcaObjectStoreException("s3 auther info cannot be empty or capitalized");
            }
            provider = new S3StoreProvider(s3config);
            this.remoteDataHome = "";
        }
        else if ("oss".equalsIgnoreCase(strorageEngine)) {
            OssConfig ossConfig = new OssConfig(antsdbConfig.getOssServiceEndpoint(), antsdbConfig.getOssRegion(),
                    antsdbConfig.getOssAccessKey(), antsdbConfig.getOssSecretKey(), antsdbConfig.getOssBucketName());
            if (ossConfig.getClientRegion() == null && ossConfig.getEndpoint() == null) {
                println("oss region and endpoint is null,please config");
                throw new OrcaObjectStoreException("oss region and endpoint is null,please config");
            }
            if (ossConfig.getAccessKey() == null || ossConfig.getSecretKey() == null) {
                println("oss auth info cannot be empty or capitalized");
                throw new OrcaObjectStoreException("s3 auther info cannot be empty or capitalized");
            }
            this.provider = new OssStoreProvider(ossConfig);
            this.remoteDataHome = "";
        }
        else {
            throw new OrcaObjectStoreException("enginee type error");
        }
    }

    private Configuration getHdfsConfig(ConfigService config) throws IOException {
        Configuration result = new Configuration();
        // add HDFS configuration if found, or use local file system
        if (config.getHdfsConf() != null) {
            String path = config.getHdfsConf();
            File hdfsConf = new File(path);
            if(!hdfsConf.exists() || !hdfsConf.isFile()) {
                hdfsConf =  new File(this.home ,path);
            }
             
            if (path != null && hdfsConf.exists()) {
                result.addResource(new Path(hdfsConf.getAbsolutePath()));
            }
            else {
                throw new OrcaObjectStoreException("hdfs configuration file is not found {}", path);
            }
        }
        String hdfsUser = config.getHdfsUser();
        result.set("dfs.client.use.datanode.hostname", "true");
        result.set("dfs.replication", "1");
        result.set("dfs.support.append", "true");
        result.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        result.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        if (hdfsUser != null && hdfsUser.length() > 0) {
            System.setProperty("HADOOP_USER_NAME", hdfsUser);
            if (result.get("dfs.user.name") == null) {
                result.set("dfs.user.name", hdfsUser);
            }
        }
        if (result.get("fs.default.name") == null || result.get("fs.default.name").endsWith("///")) {
            String uri = this.getRemoteDataHome().replaceAll(" ", "%20");
            result.set("fs.default.name", uri);
        }

        // check mandatory settings
        if (config.getHdfsUri() == null) {
            throw new OrcaObjectStoreException("hdfs uri setting 'humpback.hdfs-uri' is not configured");
        }
        // add kerberos configuration
        if (config.isKerberosEnabled()) {
            String krbconf = config.getKerberosConf();
            String principal = config.getKerberosPrincipal();
            String keytab = config.getKerberosKeytab();
            if (!new File(keytab).exists()) {
                throw new OrcaObjectStoreException("keytab file is not found {}", keytab);
            }
            if (!new File(krbconf).exists()) {
                throw new OrcaObjectStoreException("kerberos configuration file is not found {}", krbconf);
            }
            System.setProperty("java.security.krb5.conf", krbconf);

            result.set("dfs.namenode.kerberos.principal", principal);
            result.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(result);
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
        }
        return result;
    }

    public File getLocalDataHome() {
        return this.localDataHome;
    }

    private String getRemoteDataHome() {
        return remoteDataHome;
    }
}
