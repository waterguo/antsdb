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

import java.sql.Types;

import com.antsdb.saltedfish.cpp.Value;

/**
 * abstraction of database behavior
 *  
 * @author wgu0
 */
public abstract class DataTypeFactory {
	public DataTypeFactory() {
	}
	
	public DataType newDataType(String name, int length, int scale) {
        name = name.toLowerCase();
        DataType type = null;
        if ("_long".equals(name)) {
        	type = new TypeInteger("_long", Types.BIGINT, Long.class, Value.FORMAT_BIGINT, Long.MIN_VALUE, Long.MAX_VALUE);
        }
        if ("_boolean".equals(name)) {
        	type = new TypeBoolean("_boolean");
        }
		return type;
	}
}