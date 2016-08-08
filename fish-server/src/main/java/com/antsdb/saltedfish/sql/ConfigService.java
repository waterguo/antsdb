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
package com.antsdb.saltedfish.sql;

import java.io.File;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.nosql.SlowRow;
import com.antsdb.saltedfish.sql.meta.ColumnId;
import com.antsdb.saltedfish.util.UberUtil;

import static com.antsdb.saltedfish.sql.OrcaConstant.*;

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

    public void set(String key, Object value) {
        GTable table = this.orca.getHumpback().getTable(Orca.SYSNS, TABLEID_SYSPARAM);
        if (value == null) {
            table.delete(1, UberUtil.toUtf8(key), 1000);
            this.params.remove(key);
        }
        String type = value.getClass().getName();
        SlowRow row = new SlowRow(key);
        row.set(ColumnId.sysparam_name.getId(), key);
        row.set(ColumnId.sysparam_type.getId(), type);
        row.set(ColumnId.sysparam_value.getId(), value.toString());
        table.put(1, row);
        this.params.put(key, value);
    }
    
    void loadParams() {
        GTable table = this.orca.getHumpback().getTable(Orca.SYSNS, TABLEID_SYSPARAM);
        if (table == null) {
            return;
        }
        
        for (RowIterator i=table.scan(0, Integer.MAX_VALUE); i.next();) {
            long pRow = i.getRowPointer();
            if (pRow == 0) {
                break;
            }
            SlowRow row = SlowRow.fromRowPointer(orca.getSpaceManager(), pRow);
            String key = (String)row.get(ColumnId.sysparam_name.getId());
            String value = (String)row.get(ColumnId.sysparam_value.getId());
            this.params.put(key, value);
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
    
    public File getHBaseConfFile() {
    	String value = this.props.getProperty("hbase_conf", null);
    	if (value == null) {
    		return null;
    	}
    	return new File(this.orca.getHome(), value);
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
}
