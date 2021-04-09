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
import java.util.List;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;

import com.antsdb.saltedfish.nosql.ConfigService;
import com.antsdb.saltedfish.obs.aliyun.OssConfig;
import com.antsdb.saltedfish.obs.aliyun.OssStoreProvider;
import com.antsdb.saltedfish.obs.hdfs.HdfsProvider;
import com.antsdb.saltedfish.obs.s3.S3Config;
import com.antsdb.saltedfish.obs.s3.S3StoreProvider;
import com.antsdb.saltedfish.parquet.OrcaObjectStoreException;
import com.antsdb.saltedfish.parquet.ParquetDataReader;
import com.antsdb.saltedfish.parquet.ParquetUtils;
import com.antsdb.saltedfish.parquet.TableName;
import com.antsdb.saltedfish.parquet.bean.Partition;
import com.antsdb.saltedfish.parquet.bean.PartitionIndexWarp;
import com.antsdb.saltedfish.parquet.merge.MergerUtils;
import com.antsdb.saltedfish.sql.FishCommandLine;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 直接读obs中的表文件
 * @author lizc@tg-hd.com
 */
public class ObsDataReaderMain extends FishCommandLine {

    private ConfigService antsdbConfig;

    private File home;
    private String remoteDataHome;
    private File localDataHome;
    private ObsProvider provider = null;
    Configuration configuration = new Configuration();
    
    private File temp;

    public ObsDataReaderMain(String[] args) throws ParseException {
        super(args);
    }

    public static void main(String[] args) throws Exception {
        new ObsDataReaderMain(args).run();
        System.out.println("Obs data reader is completed");
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption("h", "help", false, "print help");
        options.addOption(null, "home", true, "antsdb home");
        options.addOption(null, "temp", true, "show data temp home");
        options.addOption(null, "fun", true, " show file name or data content");
        options.addOption(null, "datafile", true, "antsdb data file abs path");
        options.addOption(null, "dbname", true, "antsdb database name");
        options.addOption(null, "tbname", true, "antsdb table name");
        options.addOption(null, "tableId", true, "antsdb table id");
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
    
    public String getFunction() {
        String fun = cmd.getOptionValue("fun");
        return fun;
    }
    
    public String getDatafile() {
        String datafile = cmd.getOptionValue("datafile");
        return datafile;
    }
    
    
    public boolean isShowData() {
        return "data".equalsIgnoreCase(getFunction())?true:false;
    }
    
    public boolean isShowRowCount() {
        return "count".equalsIgnoreCase(getFunction())?true:false;
    }

    public String getTbName() {
        String tbname = cmd.getOptionValue("tbname");
        return tbname;
    }

    public String getTableId() {
        String tableId = cmd.getOptionValue("tableId");
        return tableId;
    }

    @Override
    protected String getName() {
        return null;
    }

    @Override
    protected String getCommandName() {
        return "antsdb-ObsDataReader show obs data";
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
        // read checkpoint
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
        
        String dataFile = getDatafile();
        
        if(dataFile !=null && dataFile.length() > 0 ) {
            println("show data...");
            long count =showDataFileContent(dataFile);
            println("file %s row=%s",dataFile,count);
            return;
        }
        
        String dbname = this.getDbName();
        if (dbname == null || dbname.length() == 0) {
            println("dbname is empty,please input dbname value");
            return;
        }
        else {
            println("database name is:%s",dbname);
        }
       
        String tbname = this.getTbName();
        if (tbname == null || tbname.length() == 0) {
            String databasePath = dbname;
            List<String> tableIndexFiles = this.provider.listFiles(databasePath, null,
                    ParquetUtils.DATA_JSON_EXT_NAME);
            if (tableIndexFiles != null) {
                for (String tableIndex : tableIndexFiles) {
                    if (tableIndex.endsWith("SYNCPARAM.json")) {
                        continue;
                    }
                    String tmpTableIndex = tableIndex.substring(tableIndex.lastIndexOf("/") + 1);
                     
                    String partitionIndexFileName = tmpTableIndex;
                    long count = showTableData(dbname, partitionIndexFileName);
                    println("index file %s data row=%s",tableIndex,count);
                }
            }
            else {
                println("dbname %s no data",dbname);
            }
        }
        else {
            String tableId = getTableId();
            if (tableId == null || tableId.length() == 0) {
                println("tableId is empty,please input tableId value");
                return;
            }
            long count = showData(dbname, tbname, Integer.parseInt(tableId));
            println("table=%s(%s) row=%s",tbname,tableId,count);
        }

        this.provider.close();
        this.provider = null;
        println("done");
        println("Obs data reader run is completed");
    }

    private long showDataFileContent(String dataFile) throws Exception {
        long count = 0;
        if(provider.doesObjectExist(dataFile)) {
            File filePath = new File(temp , dataFile);
            provider.downloadObject(dataFile, filePath.getAbsolutePath());
            
            try (ParquetDataReader readers = new ParquetDataReader(new Path(filePath.getAbsolutePath()), configuration)) {
                Group group = null;
                while ((group = readers.readNext()) != null) {
                    count ++;
                    String content = MergerUtils.showGroupContent(group);
                    println(content);
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
    
    private long getDataCountFileContent(String dataFile) throws Exception {
        long count = 0;
        if(provider.doesObjectExist(dataFile)) {
            File filePath = new File(temp , dataFile);
            provider.downloadObject(dataFile, filePath.getAbsolutePath());
            
            ParquetReadOptions readOption = HadoopReadOptions
                    .builder(configuration)
                    .build();
            InputFile file = HadoopInputFile.fromPath(new Path(filePath.getAbsolutePath()), configuration);
            try (ParquetFileReader fileReader = ParquetFileReader.open(file, readOption)) {
                PageReadStore page = fileReader.readNextRowGroup();
                if (page != null) {
                   return page.getRowCount();
                } 
            }
            catch (Exception e) {
                e.printStackTrace();
            }finally {
                FileUtils.forceDelete(filePath);
            }
        }
        else {
            println("data file %s not exist",dataFile);
        }
        return count;
    }
    

    private long showTableData(String dbname, String partitionIndexFileName) throws Exception {
        String objectKey = dbname + "/" + partitionIndexFileName;
        long sumCount = 0;
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
                if(isShowData()) {
                    long count = showDataFileContent(dataObjectKey); 
                    sumCount += count;
                }
                else if(isShowRowCount()) {
                    long count = getDataCountFileContent(dataObjectKey); 
                    sumCount += count;
                }
                else {
                    println(dataObjectKey);
                }
            }
        }
        return sumCount;
    }

    private long showData(String dbname, String tbname, int tableId) throws Exception {
        TableName tableInfo = TableName.valueOf(dbname, tbname, tableId);
        String path = tableInfo.getTablePath();
        List<String> lists = this.provider.listFiles(path, tableInfo.getDataFilePrefix(),
                ParquetUtils.DATA_PARQUET_EXT_NAME);

        if (lists == null || lists.size() <= 0) {
            println(" db=%s tb=%s file not searched", dbname, tbname);
            return 0;
        }
        println("show data...");
        long sumCount = 0;
        for (String tbDataFile : lists) {
            print(" db=%s tb=%s data file name=%s", dbname, tbname, tbDataFile);
            if(isShowData()) {
                long count = showDataFileContent(tbDataFile); 
                println(" count=%s",count);
                sumCount += count;
            }
            else if(isShowRowCount()) {
                long count = getDataCountFileContent(tbDataFile); 
                println(" count=%s",count);
                sumCount += count;
            }
            else {
                println(tbDataFile);
            }
        }
        return sumCount;
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
