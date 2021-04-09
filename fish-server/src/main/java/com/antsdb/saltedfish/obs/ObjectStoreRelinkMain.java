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
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.Map;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import com.antsdb.saltedfish.nosql.ConfigService;
import com.antsdb.saltedfish.obs.action.UploadSet;
import com.antsdb.saltedfish.obs.aliyun.OssConfig;
import com.antsdb.saltedfish.obs.aliyun.OssStoreProvider;
import com.antsdb.saltedfish.obs.cache.ObsCache;
import com.antsdb.saltedfish.obs.hdfs.HdfsProvider;
import com.antsdb.saltedfish.obs.hdfs.HdfsStorageService;
import com.antsdb.saltedfish.obs.s3.S3Config;
import com.antsdb.saltedfish.obs.s3.S3StoreProvider;
import com.antsdb.saltedfish.obs.upload.FishBoat;
import com.antsdb.saltedfish.parquet.CheckPoint;
import com.antsdb.saltedfish.parquet.OrcaObjectStoreException;
import com.antsdb.saltedfish.parquet.ParquetUtils;
import com.antsdb.saltedfish.parquet.TableName;
import com.antsdb.saltedfish.parquet.bean.SyncParam;
import com.antsdb.saltedfish.sql.FishCommandLine;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author lizc@tg-hd.com
 */
public class ObjectStoreRelinkMain extends FishCommandLine {

    private ConfigService antsdbConfig;
    private String sysns;
    private File home;
    private String remoteDataHome;
    private File localDataHome;
    private ObsCache obsCache;
    private ObsProvider provider = null;
    private String strorageEngine;

    public ObjectStoreRelinkMain(String[] args) throws ParseException {
        super(args);
    }

    public static void main(String[] args) throws Exception {
        new ObjectStoreRelinkMain(args).run();
        System.out.println("ObjectStore relink is completed");
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        return options;
    }

    @Override
    protected String getName() {
        return null;
    }

    @Override
    protected String getCommandName() {
        return "antsdb-storeRelink link the local AntsDB to an existing AntsDB instance in obs";
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
        strorageEngine = antsdbConfig.getStorageEngineName();
        
        this.sysns = this.antsdbConfig.getSystemNamespace();
        String localDir = antsdbConfig.getLocalData();
        this.localDataHome = new File(home,localDir);
                
        initProvider(); 
        
        println("obs provider inited");
        // read checkpoint
        println("");
        TableName tn = TableName.valueOf(this.sysns, HdfsStorageService.TABLE_SYNC_PARAM);
        CheckPoint checkPoint = new CheckPoint(tn,true);
        if (checkPoint.readSyncParam(this.obsCache) == null) {
            println("error: SYNCPARAM file is not found");
            System.exit(-1);
        }
        println("server id: %d", checkPoint.getServerId());
        println("log pointer: %x", checkPoint.getCurrentSp());

        // reset
        print("Relink will erase all data stored at your local AntsDB folder. Enter [yes] to continue: ");
        String line = new LineNumberReader(new InputStreamReader(System.in)).readLine();
        if (!line.equalsIgnoreCase("yes")) {
            println("relink is canceled");
            return;
        }
        // delete files
        File cacheFolder = new File(home, "cache");
        if (cacheFolder.exists()) {
            print("deleting %s ... ", cacheFolder);
            FileUtils.cleanDirectory(cacheFolder);
            println("done");
        }
        File dataFolder = new File(home, "data");
        if (dataFolder.exists()) {
            print("deleting %s ... ", dataFolder);
            FileUtils.cleanDirectory(dataFolder);
            println("done");
        }
        File tempFolder = new File(home, localDir);
        if (tempFolder.exists()) {
            String uploadSetObject = FishBoat.REMOTE_TMP + UploadSet.UPLOADSET_FILENAME;
            boolean exists = this.provider.doesObjectExist(uploadSetObject);
            if(exists) {
                print("deleting %s ... ", uploadSetObject);
                this.provider.deleteObject(uploadSetObject);
                println("done");
            }
            else {
                println(" %s not exists skip delete... ", uploadSetObject);
            }
            print("deleting %s ... ", tempFolder);
            FileUtils.cleanDirectory(tempFolder);
            println("done");
        }
        // create checkpoint file
        print("creating new server id ... ");
        dataFolder.mkdirs();
        File cpFile = new File(dataFolder, "checkpoint.bin");
        com.antsdb.saltedfish.nosql.CheckPoint cp = new com.antsdb.saltedfish.nosql.CheckPoint(cpFile, true);
        cp.open();
        long newServerId = cp.getServerId();
        cp.close();
        println("done");
        println("new server id: %d", newServerId);
        // setting log pointer
        println("");
        
        showConfirm();
        
        line = new LineNumberReader(new InputStreamReader(System.in)).readLine();
        if (!line.equalsIgnoreCase("yes")) {
            println("relink is canceled");
            return;
        }
        
        boolean checkExist = checkResuseExists();
        if (!checkExist) {
            println("relink is faild,System info error!!!");
            return;
        }
        
        println("setting %s checkpoint ... ",strorageEngine);
        checkPoint.setServerId(newServerId);
        checkPoint.setLogPointer(0);
        SyncParam syncParam = checkPoint.updateSyncParam();
        
        File localPath = new File(localDataHome,tn.getTablePath() );
        File file = new File(localPath,tn.getTableName() + ParquetUtils.DATA_JSON_EXT_NAME);
        String data = UberUtil.toJson(syncParam);

        FileUtils.writeStringToFile(file, data);
        if(!file.exists() || !file.isFile()) {
            println("ObjectStore relink setting %s checkpoint fail,becase file not exits... ",strorageEngine);
            return;
        }

        String objectKey = tn.getTablePath() + tn.getTableName() + ParquetUtils.DATA_JSON_EXT_NAME;
        long fsize = file.length();
        String filePath = file.getAbsolutePath();
        println("Obs provider upload localFile=%s to remoteObject=%s",filePath,objectKey);
        this.provider.uploadFile( objectKey, filePath, fsize);
        
        //re read check
        checkPoint = new CheckPoint(tn,false);
        if (checkPoint.readSyncParam(this.obsCache) == null) {
            println("error: SYNCPARAM file is not found");
            System.exit(-1);
        }
        else if (checkPoint.getServerId() != newServerId) {
            println("relink fail ,bause %s != %s",checkPoint.getServerId(), newServerId);
            System.exit(-1);
        }
        
        println("relinked server id: %d", checkPoint.getServerId());
        
        this.provider.close();
        this.obsCache.close();
        this.obsCache = null;
        this.provider = null;
        println("done");
        println("ObjectStore relink run is completed");
    }

    private void initProvider() throws Exception {
        
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
            this.remoteDataHome  = uripath;
            
            Configuration hdfsConf = getHdfsConfig(antsdbConfig);
            provider = new HdfsProvider(hdfsConf,uripath);
        }
        else if ("s3".equalsIgnoreCase(strorageEngine)) {
            S3Config s3config = new S3Config(
                    antsdbConfig.getS3ServiceEndpoint(), 
                    antsdbConfig.getS3Region(),
                    antsdbConfig.getS3accessKey(), 
                    antsdbConfig.getS3secretKey(), 
                    antsdbConfig.getS3BucketName());
            if (s3config.getClientRegion() == null 
                    || s3config.getClientRegion().length() == 0
                    || s3config.getAccessKey() == null 
                    || s3config.getSecretKey() == null) {
                throw new OrcaObjectStoreException("s3 auther info cannot be empty or capitalized");
            }
            provider = new S3StoreProvider(s3config);
            this.remoteDataHome  = "";
        }
        else if ("oss".equalsIgnoreCase(strorageEngine)) {
            OssConfig ossConfig = new OssConfig(
                    antsdbConfig.getOssServiceEndpoint(), 
                    antsdbConfig.getOssRegion(),
                    antsdbConfig.getOssAccessKey(), 
                    antsdbConfig.getOssSecretKey(), 
                    antsdbConfig.getOssBucketName());
            if (ossConfig.getClientRegion() == null && ossConfig.getEndpoint() == null) {
                println("oss region and endpoint is null,please config");
                throw new OrcaObjectStoreException("oss region and endpoint is null,please config");
            }
            if ( ossConfig.getAccessKey() == null || ossConfig.getSecretKey() == null) {
                println("oss auth info cannot be empty or capitalized");
                throw new OrcaObjectStoreException("s3 auther info cannot be empty or capitalized");
            }
            this.provider = new OssStoreProvider(ossConfig);
            this.remoteDataHome  = "";
        }
        else {
            throw new OrcaObjectStoreException("enginee type error");
        }
        
        long cacheCapacity = antsdbConfig.getObsCacheSize(); 
        
        println("init obs cache : %s",cacheCapacity);
        obsCache = new ObsCache(
                cacheCapacity,
                provider,
                this.getLocalDataHome(),
                this.getRemoteDataHome(),
                true);
    }
    
    private void showConfirm() {
        
        if ("hdfs".equalsIgnoreCase(strorageEngine)) {
        print("Confirm there is no active AntsDB instance linked to the HDFS location %s/%s. "
                + "Data will get corrupted with more than one AntsDB working on the same HDFS namespace"
                + ". Enter [yes] to continue: ", 
                antsdbConfig.getHdfsUri(),
                antsdbConfig.getSystemNamespace());
        }
        else if ("s3".equalsIgnoreCase(strorageEngine)) {
            print("Confirm there is no active AntsDB instance linked to the S3 location %s/%s. "
                    + "Data will get corrupted with more than one AntsDB working on the same S3 namespace"
                    + ". Enter [yes] to continue: ", 
                    antsdbConfig.getS3Region(), 
                    antsdbConfig.getS3BucketName());
        }
        else if ("oss".equalsIgnoreCase(strorageEngine)) {
            print("Confirm there is no active AntsDB instance linked to the oss location %s/%s. "
                    + "Data will get corrupted with more than one AntsDB working on the same oss namespace"
                    + ". Enter [yes] to continue: ", 
                    antsdbConfig.getOssRegion()!=null?antsdbConfig.getOssRegion():antsdbConfig.getOssServiceEndpoint(), 
                    antsdbConfig.getOssBucketName());
        }
        else {
            print("strorage engine ({}) not exist",strorageEngine);
        }
    }
    
    private boolean checkResuseExists() throws Exception {
        
        if ("hdfs".equalsIgnoreCase(strorageEngine)) {
            String resource = antsdbConfig.getHdfsUri();
            resource += antsdbConfig.getSystemNamespace();
            return this.provider.checkRootResourceExists(resource);
        }
        else if ("s3".equalsIgnoreCase(strorageEngine)) {
            String resource = antsdbConfig.getS3BucketName();
            return this.provider.checkRootResourceExists(resource); 
        }
        else if ("oss".equalsIgnoreCase(strorageEngine)) {
            String resource = antsdbConfig.getOssBucketName();
            return this.provider.checkRootResourceExists(resource); 
        }
        else {
            print("strorage engine ({}) not exist",strorageEngine);
        }
        return false;
    }
    
    public Configuration getHdfsConfig(ConfigService config) throws Exception {
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
                throw new OrcaObjectStoreException("hdfs configuration file is not found {}", hdfsConf.getAbsolutePath());
            }

            if (config.getProperty("dfs.client.use.datanode.hostname", null) != null
                      ||config.getProperty("fs.hdfs.impl.disable.cache", null) != null) {
                for (Map.Entry<Object, Object> i:config.getProperties().entrySet()) {
                    String key = (String)i.getKey();
                    if (key.startsWith("dfs.") || key.startsWith("fs.")) {
                        result.set(key, (String)i.getValue());
                    }
                }
            }
           
            if (result.get("fs.default.name") == null && config.getHdfsUri() == null) {
                throw new OrcaObjectStoreException("hdfs uri setting 'hdfs.uri' is not configured");
            }
            if (config.getHdfsUri() != null) {
                String uriPathStr = config.getHdfsUri();
                if (uriPathStr != null && uriPathStr.indexOf(":/") <= 0) {
                    String os = System.getProperty("os.name");
                    if (os.toLowerCase().startsWith("win")) {
                        String tmpPath = new File(uriPathStr).getAbsolutePath().replace("\\", "/");
                        uriPathStr = "file:/" + tmpPath + "/";
                    }
                    else {
                        uriPathStr = "file://" + new File(uriPathStr).getAbsolutePath();
                    }
                }
                if (uriPathStr != null && !uriPathStr.endsWith("/")) {
                    uriPathStr += "/";
                }
                this.remoteDataHome = uriPathStr;
                String uri = this.remoteDataHome.replaceAll(" ", "%20");
                result.set("fs.default.name", uri);
            }
            else {
                this.remoteDataHome = result.get("fs.default.name");
            }
            
            if (result.get("dfs.user.name") == null && config.getHdfsUser() == null) {
                throw new OrcaObjectStoreException("hdfs user setting 'hdfs.user' is not configured");
            }
            if(result.get("dfs.user.name") == null) {
                String hdfsUser = config.getHdfsUser();
                System.setProperty("HADOOP_USER_NAME", hdfsUser);
                if (result.get("dfs.user.name") == null) {
                    result.set("dfs.user.name", hdfsUser);
                }
            }
        }
        else {
            if (config.getHdfsUri() != null) {
                String uriPathStr = config.getHdfsUri();
                if (uriPathStr != null && uriPathStr.indexOf(":/") <= 0) {
                    String os = System.getProperty("os.name");
                    if (os.toLowerCase().startsWith("win")) {
                        String tmpPath = new File(uriPathStr).getAbsolutePath().replace("\\", "/");
                        uriPathStr = "file:/" + tmpPath + "/";
                    }
                    else {
                        uriPathStr = "file://" + new File(uriPathStr).getAbsolutePath();
                    }
                }
                if (uriPathStr != null && !uriPathStr.endsWith("/")) {
                    uriPathStr += "/";
                }
                this.remoteDataHome = uriPathStr;
            }
            else {
                File obsDataDirectory = new File(this.home.getAbsolutePath() , "obs");
                String uriPathStr = obsDataDirectory.getAbsolutePath();
                if (uriPathStr != null && !uriPathStr.endsWith("/")) {
                    uriPathStr += "/";
                }
                this.remoteDataHome = uriPathStr;
            }
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
