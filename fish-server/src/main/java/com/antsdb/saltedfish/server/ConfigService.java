/*-------------------------------------------------------------------------------------------------
 _______ __   _ _______ _______ ______  ______
 |_____| | \  |    |    |______ |     \ |_____]
 |     | |  \_|    |    ______| |_____/ |_____]

 Copyright (c) 2016, antsdb.com and/or its affiliates. All rights reserved. *-xguo0<@

 This program is free software: you can redistribute it and/or modify it under the terms of the
 GNU Affero General Public License, version 3, as published by the Free Software Foundation.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/agpl-3.0.txt>
-------------------------------------------------------------------------------------------------*/
package com.antsdb.saltedfish.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

public class ConfigService {
    static Logger _log = UberUtil.getThisLogger();
            
    Properties props = new Properties();
    
    public ConfigService(File file) {
        try (InputStream in = new FileInputStream(file)) {
            props.load(in);
        }
        catch (Exception x) {
            _log.warn("configuration file is not found. take everything default", x);
        }
    }

    public int getPort() {
        try {
            return Integer.valueOf(props.getProperty("fish.port"));
        }
        catch (Exception x) {
            return 3306;
        }
    }

    public String getAuthPlugin() {
    	return props.getProperty("fish.auth_plugin");
    }
    
    public String getKerberosEnable() {
        String enable = props.getProperty("kerberos.enable");
        return enable==null? "N" : enable;
    }
    
    public String getAuthEnable() {
        String enable = props.getProperty("auth.enable");
        return enable==null? "N" : enable;
    }
    
    public String getSSLKeyFile() {
        return props.getProperty("ssl.key_file");
    }
    
    // This property should be use for test case com.antsdb.saltedfish.TestCleartext only. 
    public String getTestPassword() {
        return props.getProperty("test.password");
    }
    
    public String getSSLPassword() {
        return props.getProperty("ssl.password");
    }

    public Properties getProperties() {
        return this.props;
    }

    public int getNettyWorkerThreadPoolSize() {
        try {
            return Integer.valueOf(props.getProperty("netty.worker.pool.size"));
        }
        catch (Exception x) {
        	// 0 means taking netty default
            return 0;
        }
    }
}
