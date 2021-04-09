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
package com.antsdb.saltedfish.obs.hdfs;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.ConfigService;
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
import com.antsdb.saltedfish.util.UberFormatter;
import com.antsdb.saltedfish.util.UberTime;
import com.antsdb.saltedfish.util.UberTimer;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author Frank Li<lizc@tg-hd.com>
 */

public class HdfsStorageService extends BaseStorageService {
    static Logger _log = UberUtil.getThisLogger();

    Configuration hdfsConfig = null; // hdfs configuration

    private String hdfsUrl = null;

    public HdfsStorageService(Humpback humpback) throws Exception {
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
            
            this.hdfsConfig = getHdfsConfig(antsdbConfig);
            this.provider = new HdfsProvider(hdfsConfig,hdfsUrl);

            if (antsdbConfig.getHdfsConf() == null 
                    && !this.provider.existDirectory(this.hdfsUrl)) { 
                this.provider.createDirectory(this.hdfsUrl);
            }
            else if (!this.provider.checkRootResourceExists(this.hdfsUrl)) { 
                throw new OrcaObjectStoreException(
                    "data store folder {} does not exist, please check humpback.hdfs-uri setting",
                    getRemoteDataHome());
            }
            
            initParquetConfig(antsdbConfig);
             
            _log.info("hdfs store: {} home={}",this.hdfsUrl, this.getRemoteDataHome());

            boatThreads = new ExecutorBoatPool();
            catchThreads = new ExecutorCatchPool(antsdbConfig.getUplaodThreadCount());
            
            init();
            
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
                        _log.debug("ext hdfs config:{}",key);
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
                this.hdfsUrl = uriPathStr;
                String uri = this.hdfsUrl.replaceAll(" ", "%20");
                result.set("fs.default.name", uri);
            }
            else {
                this.hdfsUrl = result.get("fs.default.name");
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
                this.hdfsUrl = uriPathStr;
            }
            else {
                File obsDataDirectory = new File(this.home.getAbsolutePath() , "obs");
                String uriPathStr = obsDataDirectory.getAbsolutePath();
                if (uriPathStr != null && !uriPathStr.endsWith("/")) {
                    uriPathStr += "/";
                }
                this.hdfsUrl = uriPathStr;
            }
        }
        // 这个解决hdfs问题
        result.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        // 这个解决本地file问题
        result.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

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
            _log.info("kerberos is enabled with krb={} principal={} keytab={}", krbconf, principal, keytab);
            System.setProperty("java.security.krb5.conf", krbconf);

            result.set("dfs.namenode.kerberos.principal", principal);
            result.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(result);
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
        }
        return result;
    }

    public void waitForSync(int timeoutSeconds) throws TimeoutException {
        SpaceManager spaceman = this.humpback.getSpaceManager();
        // find out the current space pointer
        long spNow = spaceman.getAllocationPointer();
        // write a bogus rollback so that spNow can be replayed
        HumpbackSession hsession = this.humpback.createSession(":HdfsStorageService");
        try {
            this.humpback.setConfig(hsession, "waitForSync: {}", UberTime.getTime());
        }
        finally {
            this.humpback.deleteSession(hsession);
        }
        // wait until timeout
        _log.debug("waitForSync: ", UberFormatter.hex(spNow));
        UberTimer timer = new UberTimer(timeoutSeconds * 1000);
        for (;;) {
            if (getCommittedLogPointer() >= spNow ) {
                // _log.debug("waitForSync :CurrentSP:{},spNow:{}",getCurrentSP(),spNow);
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

//    @Override
//    public String getRemoteDataHome() {
//        return hdfsUrl;
//    }

    @Override
    public Configuration getHdfsConfiguration() {
        return this.hdfsConfig;
    }
}