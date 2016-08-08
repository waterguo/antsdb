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
package com.antsdb.saltedfish.minidb;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * minidb is small, high performance and robust key value database for small volume of data. it is used to keep local 
 * metadata 
 *  
 * @author wgu0
 */
public class MiniDb implements AutoCloseable {
	File file;
	
	public MiniDb(File file) {
		super();
		this.file = file;
		load();
	}

	public Map<String, Object> get(int key) {
		return null;
	}
	
	public List<Map<String, Object>> getAll() {
		return null;
	}
	
	public void put(int key, Map<String, Object> row) {
	}
	
	public void delete(int key) {
	}
	
	public void save() {
	}

	private void load() {
	}
	
	@Override
	public void close() throws Exception {
	}
}
