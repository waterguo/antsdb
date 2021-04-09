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
package com.antsdb.saltedfish.nosql;

import java.io.File;
import java.io.FileInputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import com.antsdb.saltedfish.minke.AllPartialStrategy;
import com.antsdb.saltedfish.minke.CacheStrategy;
import com.antsdb.saltedfish.util.Size;
import com.antsdb.saltedfish.util.SizeConstants;
import com.antsdb.saltedfish.util.TimeParser;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author wgu0
 */
public class ConfigService {
    static final int KB = 1024;
    static final int MB = 1024 * KB;
    static final int GB = 1024 * MB;

    static Logger _log = UberUtil.getThisLogger();

    Properties props;
    File file;

    public ConfigService() {
        this.props = new Properties();
    }

    public ConfigService(File file) throws Exception {
        this.file = file;
        this.props = new Properties();
        if (file.exists()) {
            try (FileInputStream in = new FileInputStream(file)) {
                props.load(in);
            }
        }
    }

    public boolean isHbaseOn() {
        return getStorageEngineName().equals("hbase");
    }

    public boolean isValidationOn() {
        String value = this.props.getProperty("humpback.data.validation", "false");
        try {
            return Boolean.parseBoolean(value);
        }
        catch (Exception x) {
        }
        return false;
    }

    public boolean isLogWriterEnabled() {
        String value = this.props.getProperty("humpback.log.writer", "true");
        try {
            return Boolean.parseBoolean(value);
        }
        catch (Exception x) {
        }
        return true;
    }

    public int getSpaceFileSize() {
        return (int) Size.parse(this.props.getProperty("humpback.space.file.size"), "256m");
    }

    public String getProperty(String key, String defaultValue) {
        return this.props.getProperty(key, defaultValue);
    }

    public int getHBaseBufferSize() {
        return getInt("hbase_buffer_size", 2000);
    }

    public int getHBaseMaxColumnsPerPut() {
        return getInt("hbase_max_column_per_put", 2500);
    }

    public String getHBaseCompressionCodec() {
        return this.props.getProperty("hbase_compression_codec", "GZ");
    }

    public String getKrbRealm() {
        return this.props.getProperty("krb_realm", "");
    }

    public String getKrbKdc() {
        return this.props.getProperty("krb_kdc", "");
    }

    public String getKrbJaasConf() {
        String value = this.props.getProperty("krb_jaas", null);
        if (value == null) {
            return null;
        }
        File f = new File(value);

        if (f.getAbsoluteFile().exists())
            return f.getAbsoluteFile().toString();
        else
            return null;
    }

    public int getMinkePageSize() {
        return (int) Size.parse(this.props.getProperty("minke.page-size"), "16m");
    }

    public int getMinkeFileSize() {
        return (int) Size.parse(this.props.getProperty("minke.file-size"), "1g");
    }

    public long getMinkeSize() {
        return Size.parse(this.props.getProperty("minke.size"), String.valueOf(Long.MAX_VALUE));
    }

    public long getCacheSize() {
        return Size.parse(this.props.getProperty("cache.size"), "100g");
    }

    public CacheStrategy getCacheStrategy() {
        String klass = this.props.getProperty("cache.strategy", "AllPartial");
        klass = "com.antsdb.saltedfish.minke." + klass + "Strategy";
        try {
            return (CacheStrategy) Class.forName(klass).newInstance();
        }
        catch (Exception x) {
            _log.warn("invalid setting for cache.strategy", x);
            return new AllPartialStrategy();
        }
    }

    private long getSeconds(String value, String defecto) {
        return TimeParser.parseSeconds(value, defecto);
    }

    @SuppressWarnings("unused")
    private long getLong(String key, long defaultValue) {
        long value = defaultValue;
        String s = this.props.getProperty(key);
        if (s != null && s.trim() != "") {
            value = Long.parseLong(s);
        }
        return value;
    }

    private int getInt(String key, int defaultValue) {
        int value = defaultValue;
        String s = this.props.getProperty(key);
        if (s != null && s.trim() != "") {
            value = Integer.parseInt(s);
        }
        return value;
    }

    private boolean getBoolean(String key, boolean defaultValue) {
        boolean value = defaultValue;
        String s = this.props.getProperty(key);
        if (s != null && s.trim() != "") {
            value = Boolean.parseBoolean(s);
        }
        return value;
    }

    public boolean isSynchronizerEnabled() {
        return getBoolean("minke.synchronizer", true);
    }

    /**
     * when recycling files, rename them to ".junk" instead of deleting from
     * file system
     * 
     * @return
     */
    public boolean isFakeDeletetionEnabled() {
        return getBoolean("humpback.fake-deletion", false);
    }

    public Properties getProperties() {
        return this.props;
    }

    public int getTabletSize() {
        return (int) Size.parse(this.props.getProperty("humpback.tablet.file.size"), "64m");
    }

    public String getStorageEngineName() {
        String result = this.props.getProperty("humpback.storage-engine");
        return (result == null) ? "minke" : result;
    }

    public String getHBaseConf() {
        // unbelievable, must call trim() otherwise kerberos will break with accidental white space
        String result = StringUtils.trim(this.props.getProperty("humpback.hbase-conf"));
        return result;
    }

    /**
     * is cache verification enabled ?
     * 
     * @return 0:diabled; 1:only after fetch; 2:always
     */
    public int getCacheVerificationMode() {
        return getInt("cache.verification-mode", 0);
    }

    public String getSystemNamespace() {
        String result = props.getProperty("humpback.hbase-system-ns");
        return result != null && result.length() > 0 ? result : "ANTSDB";
    }

    public String getDefaultDatabaseType() {
        try {
            String s = props.getProperty("orca.defaultDatabaseType", "mysql").toUpperCase();
            return s;
        }
        catch (Exception x) {
            return "MYSQL";
        }
    }

    public LogRetentionStrategy getLogRetentionStrategy() {
        String value = props.getProperty("humpback.log-retention", "").toLowerCase();
        if ("time".equals(value)) {
            // default retain log for 7 days
            long time = getSeconds("humpback.log-retention.time", "1w");
            return new LogRetentionByTime(time);
        }
        else if ("size".equals(value)) {
            long bytes = Size.parse(this.props.getProperty("humpback.log-retention.size"), "1g");
            return new LogRetentionBySize(bytes);
        }
        else {
            // default retain log for 1 gb
            return new LogRetentionBySize(1024 * 1024 * 1024);
        }
    }

    public boolean isSlaveEnabled() {
        return getBoolean("humpback.slave", false);
    }

    public String getSlaveUrl() {
        return this.props.getProperty("humpback.slave.url");
    }

    public String getSlaveUser() {
        return this.props.getProperty("humpback.slave.user");
    }

    public String getSlavePassword() {
        return this.props.getProperty("humpback.slave.password");
    }

    public long getCacheEvictorTarget() {
        long result = Size.parse(this.props.getProperty("cache.evictor.target"), "10g");
        return result;
    }

    public long getWarmerSize() {
        long result = Size.parse(this.props.getProperty("humpback.warmer.size"), "0");
        return result;
    }

    public int getLatencyDetectionMs() {
        String value = this.props.getProperty("humpback.latency-detection-ms");
        if (value != null) {
            try {
                return Integer.parseInt(value);
            }
            catch (Exception ignored) {
            }
        }
        return 0;
    }

    public boolean isCrashSceneEnabled() {
        return getBoolean("humpback.crash-scene", false);
    }

    public String getZookeeperConnectionString() {
        return this.props.getProperty("cluster.zookeeper", "localhost:2181");
    }

    public boolean isClusterEnabled() {
        return getClusterName() != null;
    }
    
    public String getClusterName() {
        return this.props.getProperty("cluster.name", null);
    }

    /**
     * reserved space for stuff read from hbase. 
     * 
     * @return
     */
    public long getCacheReservedSize() {
        return getLong("cache.reserved-size", SizeConstants.gb(1));
    }

    public long getServerId() {
        return getLong("server-id", -1);
    }

    public boolean isKerberosEnabled() {
        return getBoolean("kerberos", false);
    }

    /**
     * location of the krb5 config file
     * @return null if not found
     */
    public String getKerberosConf() {
        return this.props.getProperty("kerberos.krb5-conf");
    }

    public String getKerberosPrincipal() {
        return this.props.getProperty("kerberos.principal");
    }

    public String getKerberosKeytab() {
        return this.props.getProperty("kerberos.keytab");
    }

    /**
     * root directory of antsdb files in the hdfs 
     * @return
     */
    public String getHdfsUri() {
        String result = StringUtils.trim(this.props.getProperty("hdfs.uri"));
        if(!result.endsWith("/") && !result.endsWith("\\")) {
            result += "/";
        }
        return result;
    }

    public String getDatafileCompressionCodec() {
        return this.props.getProperty("humpback.parquet-compressioncodec", "UNCOMPRESSED");
    }

    public String getHdfsUser() {
        String result = StringUtils.trim(this.props.getProperty("hdfs.user", "hdfs"));
        return result;
    }

    /**
     * location of the hdfs-site.xml file
     * @return
     */
    public String getHdfsConf() {
        String result = StringUtils.trim(this.props.getProperty("hdfs.site-conf"));
        return result;
    }
    
    public String getPartitionMaxRowCount() {
        String result = StringUtils
                .trim(this.props.getProperty("humpback.parquet-partition-threshold-maxrowcount", "100000"));
        return result;
    }

    public String getPartitionMaxDatasize() {
        String result = StringUtils
                .trim(this.props.getProperty("humpback.parquet-partition-threshold-maxdatasize", "40"));
        return result;
    }
    /**
     * store new create parquet file or merger parquet file or merged parquet file
     * @return
     */
    public String getLocalData() {
        String result = StringUtils.trim(this.props.getProperty("humpback.parquet-datafile-localdata", "temp/obs-cache/"));
        if (!result.startsWith("temp")) {
            result = "temp/" + result;
        }
        return result;
    }

    public String getS3Region() {
        String result = StringUtils.trim(this.props.getProperty("s3.client-region"));
        return result;
    }

    public String getS3accessKey() {
        String result = StringUtils.trim(this.props.getProperty("s3.client-accessKey", ""));
        return result;
    }

    public String getS3secretKey() {
        String result = StringUtils.trim(this.props.getProperty("s3.client-secretKey", ""));
        return result;
    }

    public String getS3ServiceEndpoint() {
        String result = StringUtils.trim(this.props.getProperty("s3.client-serviceEndpoint"));
        return result;
    }

    public String getS3BucketName() {
        String result = StringUtils.trim(this.props.getProperty("s3.client-bucketName"));
        return result;
    }

    public String getAthenaEnable() {
        String result = StringUtils.trim(this.props.getProperty("athena.enable", "false"));
        return result;
    }
    
    /**
     * Fix cn Region athean suffix error
     * athena.endpointOverride = athena.cn-northwest-1.amazonaws.com.cn:443
     * @return
     */
    public String getAthenaEndpointOverride() {
        String result = StringUtils.trim(this.props.getProperty("athena.endpointOverride"));
        return result;
    }
    
    public long getSyncBatchSize() {
        return Size.parse(this.props.getProperty("humpback.sync.batchsize"), "128m");
    }

    /**
     * batch size used in OBS replication
     * 
     * @return
     */
    public long getObsCacheSize() {
        return Size.parse(this.props.getProperty("humpback.obs-cache-size"), "10G");
    }

    /**
     * port used to intercommunications in a cluster
     * @return
     */
    public int getAuxPort() {
        try {
            return Integer.valueOf(props.getProperty("fish.aux-port", "2007").trim());
        }
        catch (Exception x) {
            return 2007;
        }
    }

    public String getAuxEndpoint() {
        try {
            int port = getAuxPort();
            String endpoint;
            endpoint = InetAddress.getLocalHost().getHostName() + ":" + port;
            return endpoint;
        }
        catch (UnknownHostException e) {
            return "";
        }
    }

    public String getSparksqlEnable() {
        String result = StringUtils.trim(this.props.getProperty("spark.enable", "false"));
        return result;
    }

    public String getSparkHome() {
        String result = StringUtils.trim(this.props.getProperty("spark.home", "./spark"));
        return result;
    }

    public String getSparkMaster() {
        String result = StringUtils.trim(this.props.getProperty("spark.master", "local"));
        return result;
    }

    public String getSparkMode() {
        String result = StringUtils.trim(this.props.getProperty("spark.mode", "client"));
        return result;
    }

    public String getSparkTaskJar() {
        String result = StringUtils.trim(this.props.getProperty("spark.taskjar", "lib/fish-spark-1.3.jar"));
        return result;
    }

    public String getSparkResultPath() {
        String result = StringUtils.trim(this.props.getProperty("spark.result-path", "temp"));
        return result;
    }

    public String getHadoopConfigDir() {
        String result = StringUtils.trim(this.props.getProperty("spark.hadoop-conf-dir", "conf"));
        return result;
    }
    
    public int getUplaodThreadCount() {
        try {
            String val = props.getProperty("humpback.obs-upload.threadcount","4").trim();
            int uploadThreadCount = Integer.valueOf(val);
            if (uploadThreadCount < 1) {
                uploadThreadCount = 2;
            }
            return uploadThreadCount;
        }
        catch (Exception e) {
            _log.error(e.getMessage(), e);
        }
        return Runtime.getRuntime().availableProcessors() / 2;
    }
    
    /**
     * aliyun oss config
     * @return
     */
    public String getOssRegion() {
        String result = StringUtils.trim(this.props.getProperty("oss.region"));
        return result;
    }

    public String getOssAccessKey() {
        String result = StringUtils.trim(this.props.getProperty("oss.accessKey", ""));
        return result;
    }

    public String getOssSecretKey() {
        String result = StringUtils.trim(this.props.getProperty("oss.secretKey", ""));
        return result;
    }

    public String getOssServiceEndpoint() {
        String result = StringUtils.trim(this.props.getProperty("oss.serviceEndpoint"));
        return result;
    }
    
    public String getOssBucketName() {
        String result = StringUtils.trim(this.props.getProperty("oss.bucketName"));
        return result;
    }
    
    public String getDataLakeAnalyticsEnable() {
        String result = StringUtils.trim(this.props.getProperty("dataLakeAnalytics.enable", "false"));
        return result;
    }
    
    public String getDataLakeUrl() {
        String result = StringUtils.trim(this.props.getProperty("dataLakeAnalytics.url", ""));
        return result;
    }
    
    public String getDataLakeUser() {
        String result = StringUtils.trim(this.props.getProperty("dataLakeAnalytics.user", ""));
        return result;
    }
    
    public String getDataLakePasswd() {
        String result = StringUtils.trim(this.props.getProperty("dataLakeAnalytics.passwd", ""));
        return result;
    }

    public boolean getMd5Flag() {
        String value = this.props.getProperty("humpback.parquet-md5", "false");
        try {
            return Boolean.parseBoolean(value);
        }
        catch (Exception x) {
        }
        return false;
    }

    public int getMaxSqlLengthInLog() {
        return getInt("orca.max-sql-length-in-log", 2048);
    }
    
    /*
     * used to log server side sql statements. set this option to "write" to enable it. currently it only logs
     * DML and DDL which changes the database  
     */
    public String getSqlLogOption() {
        return this.props.getProperty("orca.sql-trace");
    }

    /**
     * sql trace file size. default is 50m 
     */
    public int getSqlLogFileSize() {
        return (int)Size.parse(this.props.getProperty("orca.sql-trace.file-size"), "50m");
    }

    /**
     * max sql trace entry size. default is 2k 
     */
    public int getSqlLogMaxEntrySize() {
        return (int)Size.parse(this.props.getProperty("orca.sql-trace.max-entry-size"), "2k");
    }
}
