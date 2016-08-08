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
package com.antsdb.saltedfish.sql.mysql;

import java.sql.Types;

import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.lexer.MysqlParser.Data_typeContext;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.DataTypeFactory;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.TypeBlob;
import com.antsdb.saltedfish.sql.TypeClob;
import com.antsdb.saltedfish.sql.TypeDate;
import com.antsdb.saltedfish.sql.TypeDecimal;
import com.antsdb.saltedfish.sql.TypeDouble;
import com.antsdb.saltedfish.sql.TypeFloat;
import com.antsdb.saltedfish.sql.TypeInteger;
import com.antsdb.saltedfish.sql.TypeString;
import com.antsdb.saltedfish.sql.TypeTimestamp;

/**
 * factory of mysql data types
 *  
 * @author wgu0
 */
public class MysqlDataTypeFactory extends DataTypeFactory {
	
	@Override
	public DataType newDataType(String name, int length, int scale) {
		DataType type = super.newDataType(name, length, scale);
		return  (type == null) ?  newDataType_(name, length, scale) : type;
	}

	public static DataType newDataType_(String name, int length, int scale) {
        name = name.toLowerCase();
        DataType type = null;
        if ("char".equals(name)) {
            type = new TypeString("char", length, Types.CHAR);
        }
        else if ("varchar".equals(name)) {
            type = new TypeString("varchar", length, Types.VARCHAR);
        }
        else if ("float".equals(name)) {
            type = new TypeFloat("float");
        }
        else if ("double".equals(name)) {
            type = new TypeDouble("double");
        }
        else if ("real".equals(name)) {
            type = new TypeDouble("double");
        }
        else if ("decimal".equals(name)) {
            type = new TypeDecimal("decimal", length, scale);
        }
        else if ("datetime".equals(name)) {
            type = new TypeTimestamp("datetime", 0);
        }
        else if ("timestamp".equals(name)) {
            type = new TypeTimestamp("timestamp", 0);
        }
        else if ("text".equals(name)) {
            type = new TypeClob("text", Types.CLOB, 0xffff);
        }
        else if ("tinytext".equals(name)) {
            type = new TypeClob("tinytext", Types.CLOB, 0xff);
        }
        else if ("mediumtext".equals(name)) {
            type = new TypeClob("mediumtext", Types.CLOB, 0xffffff);
        }
        else if ("longtext".equals(name)) {
            type = new TypeClob("longtext", Types.CLOB, 0xffffffffl);
        }
        else if ("blob".equals(name)) {
            type = new TypeClob("blob", Types.BLOB, 0xffff);
        }
        else if ("tinyblob".equals(name)) {
            type = new TypeClob("tinyblob", Types.BLOB, 0xff);
        }
        else if ("mediumblob".equals(name)) {
            type = new TypeClob("mediumblob", Types.BLOB, 0xffffff);
        }
        else if ("longblob".equals(name)) {
            type = new TypeBlob("longblob", Types.BLOB, 0xffffffffl);
        }
        else if ("binary".equals(name)) {
        	type = new TypeBlob("binary", Types.BLOB, 0xffffffffl);
        }
        else if ("varbinary".equals(name)) {
        	type = new TypeBlob("binary", Types.BLOB, 0xffffffffl);
        }
        else if ("bool".equals(name) || "boolean".equals(name)) {
            type = new TypeInteger(
            		"tinyint", 
            		Types.TINYINT, 
            		Integer.class, 
            		Value.TYPE_NUMBER, 
            		127, 
            		-128);
            type.setLength(1);
        }
        else if ("tinyint".equals(name)) {
            type = new TypeInteger(
            		"tinyint", 
            		Types.TINYINT, 
            		Integer.class, 
            		Value.TYPE_NUMBER, 
            		127, 
            		-128);
            type.setLength(length);
        }
        else if ("smallint".equals(name)) {
            type = new TypeInteger(
            		"smallint", 
            		Types.SMALLINT, 
            		Integer.class, 
            		Value.TYPE_NUMBER, 
            		32767, 
            		-32768); 
        }
        else if ("mediumint".equals(name)) {
            type = new TypeInteger(
            		"mediumint", 
            		Types.INTEGER, 
            		Integer.class, 
            		Value.TYPE_NUMBER, 
            		8388607, 
            		-8388608); 
        }
        else if ("int".equals(name) || "integer".equals(name)) {
            type = new TypeInteger(
            		"int", 
            		Types.INTEGER, 
            		Integer.class, 
            		Value.TYPE_NUMBER, 
            		Integer.MAX_VALUE, 
            		Integer.MIN_VALUE + 1); 
            type.setLength(length);
        }
        else if ("bigint".equals(name)) {
            type = new TypeInteger(
            		"bigint", 
            		Types.BIGINT, 
            		Long.class, 
            		Value.TYPE_NUMBER, 
            		Long.MAX_VALUE, 
            		Long.MIN_VALUE + 1); 
        }
        else if ("date".equals(name)) {
        	type = new TypeDate("date");
        }
        else if ("enum".equals(name)) {
            type = new TypeInteger(
            		"enum", 
            		Types.INTEGER, 
            		Integer.class, 
            		Value.TYPE_NUMBER, 
            		65535,
            		0); 
        }
        else {
            throw new OrcaException(902, "invalid data type: " + name);
        }
        return type;
    }

    public static DataType parse(Data_typeContext rule) {
        String typeName = null;
        int length = 0;
        int scale = 0;
        if (rule.data_type_nothing() != null) {
            typeName = rule.data_type_nothing().any_name().getText();
        }
        else if (rule.data_type_length() != null) {
            typeName = rule.data_type_length().any_name().getText();
            length = Integer.parseInt(rule.data_type_length().signed_number().getText());
        }
        else if (rule.data_type_length_scale() != null) {
            typeName = rule.data_type_length_scale().any_name().getText();
            length = Integer.parseInt(rule.data_type_length_scale().signed_number(0).getText());
            scale = Integer.parseInt(rule.data_type_length_scale().signed_number(1).getText());
        }
        return newDataType_(typeName, length, scale);
    }

}