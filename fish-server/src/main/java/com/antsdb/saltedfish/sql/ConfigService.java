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
package com.antsdb.saltedfish.sql;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.nosql.HumpbackSession;

public class ConfigService {
    Properties props;
    Orca orca;
    Map<String, Object> params = new ConcurrentHashMap<String, Object>();
    
    public ConfigService(Orca orca, Properties props) {
        this.orca = orca;
        this.props = props;
        loadParams();
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

    public Object get(String key) {
        return this.params.get(key);
    }
    
    public void set(HumpbackSession hsession, String key, String value) {
        this.orca.getHumpback().setConfig(hsession, key, value);
        if (value == null) {
            this.params.remove(key);
        }
        else {
            this.params.put(key, value);
        }
    }
    
    void loadParams() {
        for (Map.Entry<String, String> i:this.orca.getHumpback().getAllConfig().entrySet()) {
            this.params.put(i.getKey(), i.getValue());
        }
    }

    public String getDatabaseType() {
        String type = (String)this.params.get("databaseType");
        return (type != null) ? type : "MYSQL";
    }
    
    public String[] getClusterNodes() {
        String value = (String)this.props.get("orca.cluster");
        if (StringUtils.isEmpty(value)) {
            return new String[0];
        }
        return StringUtils.split(value, ',');
    }
    
    public String getThisNode() {
        String value = (String)this.props.get("orca.node");
        return value;
    }
    
    public int getSlaveServerId() {
        int value = Integer.parseInt(this.props.getProperty("slave.server-id"));
        return value;
    }

    public String getSlaveUser() {
        return this.props.getProperty("masterUser");
    }

    public String getSlavePassword() {
        return this.props.getProperty("masterPassword");
    }
    
    public boolean isAsynchronousImportEnabled() {
        return getBoolean("orca.asynchronous-import", true);
    }

    private boolean getBoolean(String key, boolean defaultValue) {
        boolean value = defaultValue;
        String s = this.props.getProperty(key);
        if (s != null && s.trim() != "") {
            value = Boolean.getBoolean(s);
        }
        return value;
    }

    public String getMasterHost() {
        return this.props.getProperty("masterHost");
    }

    public int getMasterPort() {
        return getInt("masterPort", 3306);    
    }
    
    private int getInt(String key, int defaultValue) {
        int value = defaultValue;
        String s = this.props.getProperty(key);
        if (s != null && s.trim() != "") {
            value = Integer.parseInt(s);
        }
        return value;
    }

    public Set<String> getIgnoreList() {
        String value = this.props.getProperty("slave.ignore-db");
        if (value == null) {
            return Collections.emptySet();
        }
        Set<String> result = new HashSet<>();
        for (String i:StringUtils.split(value, ",")) {
            result.add(i);
        }
        return result;
    }
}
