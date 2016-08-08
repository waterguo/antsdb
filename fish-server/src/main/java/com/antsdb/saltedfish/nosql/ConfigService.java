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
package com.antsdb.saltedfish.nosql;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

/**
 * 
 * @author wgu0
 */
public class ConfigService {
    Properties props;
    File file;

    ConfigService(File file) throws Exception {
    	this.file = file;
		this.props = new Properties();
		if (file.exists()) {
			try (FileInputStream in = new FileInputStream(file)) {
				props.load(in);
			}
		}
	}

    public File getHBaseConfFile() {
    	String value = this.props.getProperty("hbase_conf", null);
    	if (value == null) {
    		return null;
    	}
    	return new File(this.file.getParent(), value);
    }

	public boolean isValidationOn() {
    	String value = this.props.getProperty("humpback.data.validation", "false");
    	try {
    		return Boolean.parseBoolean(value);
    	}
    	catch (Exception x) {}
    	return false;
	}

	public boolean isLogWriterEnabled() {
    	String value = this.props.getProperty("humpback.log.writer", "true");
    	try {
    		return Boolean.parseBoolean(value);
    	}
    	catch (Exception x) {}
    	return true;
	}

	public int getSpaceFileSize() {
    	String value = this.props.getProperty("humpback.space.file.size", "256");
    	try {
    		return Integer.parseInt(value) * 1024 * 1024;
    	}
    	catch (Exception x) {}
    	return 256 * 1024 * 1024;
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

	private int getInt(String key, int defaultValue) {
		int value = defaultValue;
		String s = this.props.getProperty(key);
		if (s != null && s.trim() != "") {
			value = Integer.parseInt(s);
		}
		return value;
	}
	
}
