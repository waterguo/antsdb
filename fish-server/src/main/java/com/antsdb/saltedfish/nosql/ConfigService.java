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
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import com.antsdb.saltedfish.minke.AllButBlobStrategy;
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
        return (int)Size.parse(this.props.getProperty("humpback.space.file.size"), "256m");
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
        return (int)Size.parse(this.props.getProperty("minke.page-size"), "16m");
    }

    public int getMinkeFileSize() {
        return (int)Size.parse(this.props.getProperty("minke.file-size"), "1g");
    }

    public long getMinkeSize() {
        return Size.parse(this.props.getProperty("minke.size"), String.valueOf(Long.MAX_VALUE));
    }

    public long getCacheSize() {
        return Size.parse(this.props.getProperty("cache.size"), "100g");
    }
    
    public CacheStrategy getCacheStrategy() {
        String klass = this.props.getProperty("cache.strategy", "AllButBlob");
        klass = "com.antsdb.saltedfish.minke." + klass + "Strategy";
        try {
            return (CacheStrategy)Class.forName(klass).newInstance();
        }
        catch (Exception x) {
            _log.warn("invalid setting for cache.strategy", x);
            return new AllButBlobStrategy();
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
        return (int)Size.parse(this.props.getProperty("humpback.tablet.file.size"), "64m");
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
        return result != null ? result : "ANTSDB";
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
            catch (Exception ignored) {}
        }
        return 0;
    }

    public boolean isCrashSceneEnabled() {
        return getBoolean("humpback.crash-scene", false);
    }
    
    public String getZookeeperConnectionString() {
        return "baozi:2181";
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
}
