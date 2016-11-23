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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;

/**
 * 
 * @author *-xguo0<@
 */
public class SessionParameters {
    Map<String, Object> parameters = new ConcurrentHashMap<String, Object>();
    boolean no_auto_value_on_zero;
    // oracle default is 30 minutes
	// mysql default is 50 seconds. innodb_lock_wait_timeout
    int lock_timeout = 50 * 1000;
    
	public Map<String, Object> getParameters() {
		return Collections.unmodifiableMap(this.parameters);
	}
	
	public void setParameter(String name, Object value) {
		String key = name.toLowerCase();
    	if (value != null) {
    		this.parameters.put(key, value);
    	}
    	else {
    		this.parameters.remove(key);
    	}
		if (key.equals("sql_mode")) {
			parseSqlModes((String)value);
		}
		else if (key.equals("ddl_lock_timeout") || key.equals("innodb_lock_wait_timeout")) {
			if (value != null) {
				this.lock_timeout = ((Number)value).intValue() * 1000;
			}
			else {
        		this.lock_timeout = 50 * 1000;
			}
		}
	}
	
	private void parseSqlModes(String value) {
		this.no_auto_value_on_zero = false;
		if (StringUtils.isEmpty(value)) {
			return;
		}
		for (String mode : StringUtils.split(value, ',')) {
			if (mode.equals("NO_AUTO_VALUE_ON_ZERO")) {
				this.no_auto_value_on_zero = true;
			}
			else if (mode.equals("STRICT_TRANS_TABLES")) {
			}
			else {
				throw new OrcaException("unknown sql mode " + mode);
			}
		}
	}

	/**
	 * {@link} http://dev.mysql.com/doc/refman/5.7/en/sql-mode.html#sqlmode_no_auto_value_on_zero	
	 * @return
	 */
	public boolean isNoAutoValueOnZero() {
		return this.no_auto_value_on_zero;
	}
	
	public int getLockTimeout() {
		return this.lock_timeout;
	}

	public Object get(String name) {
		name = name.toLowerCase();
		return this.parameters.get(name);
	}
}
