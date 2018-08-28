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
package com.antsdb.saltedfish.sql.mysql;

import java.sql.Types;

import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.lexer.MysqlParser.Data_typeContext;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.DataTypeFactory;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.TypeBinary;
import com.antsdb.saltedfish.sql.TypeBlob;
import com.antsdb.saltedfish.sql.TypeClob;
import com.antsdb.saltedfish.sql.TypeDate;
import com.antsdb.saltedfish.sql.TypeDecimal;
import com.antsdb.saltedfish.sql.TypeDouble;
import com.antsdb.saltedfish.sql.TypeFloat;
import com.antsdb.saltedfish.sql.TypeInteger;
import com.antsdb.saltedfish.sql.TypeString;
import com.antsdb.saltedfish.sql.TypeTime;
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
        if (type == null) {
            type = newDataType_(name, length, scale);
        }
        if (name.endsWith(" unsigned")) {
            type.setUnsigned(true);
        }
        return type;
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
        else if ("float unsigned".equals(name)) {
            type = new TypeFloat("float unsigned");
        }
        else if ("double".equals(name)) {
            type = new TypeDouble("double");
        }
        else if ("double unsigned".equals(name)) {
            type = new TypeDouble("double unsigned");
        }
        else if ("real".equals(name)) {
            type = new TypeDouble("double");
        }
        else if ("real unsigned".equals(name)) {
            type = new TypeDouble("double unsigned");
        }
        else if ("decimal".equals(name)) {
            type = new TypeDecimal("decimal", length, scale);
        }
        else if ("datetime".equals(name)) {
            type = new TypeTimestamp("datetime", length);
        }
        else if ("timestamp".equals(name)) {
            type = new TypeTimestamp("timestamp", length);
        }
        else if ("time".equals(name)) {
            type = new TypeTime("time");
        }
        else if ("text".equals(name)) {
            type = new TypeString("text", Types.CLOB, 0xffff);
        }
        else if ("tinytext".equals(name)) {
            type = new TypeString("tinytext", Types.CLOB, 0xff);
        }
        else if ("mediumtext".equals(name)) {
            type = new TypeClob("mediumtext", Types.CLOB, 0xffffff);
        }
        else if ("longtext".equals(name)) {
            type = new TypeClob("longtext", Types.CLOB, 0xffffffffl);
        }
        else if ("blob".equals(name)) {
            type = new TypeBinary("blob", Types.BLOB, 0xffff);
        }
        else if ("tinyblob".equals(name)) {
            type = new TypeBinary("tinyblob", Types.BLOB, 0xff);
        }
        else if ("mediumblob".equals(name)) {
            type = new TypeBlob("mediumblob", Types.BLOB, 0xffffff);
        }
        else if ("longblob".equals(name)) {
            type = new TypeBlob("longblob", Types.BLOB, 0xffffffffl);
        }
        else if ("binary".equals(name)) {
            type = new TypeBinary("binary", Types.BINARY, 0xffffffffl);
        }
        else if ("varbinary".equals(name)) {
            type = new TypeBinary("varbinary", Types.VARBINARY, 0xffffffffl);
        }
        else if ("bool".equals(name) || "boolean".equals(name)) {
            type = new TypeInteger(
                    "tinyint", 
                    Types.TINYINT, 
                    Integer.class, 
                    Value.TYPE_NUMBER,
                    0,
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
                    length,
                    127, 
                    -128);
            type.setLength(length);
        }
        else if ("tinyint unsigned".equals(name)) {
            type = new TypeInteger(
                    "tinyint unsigned", 
                    Types.TINYINT, 
                    Integer.class, 
                    Value.TYPE_NUMBER,
                    length,
                    255,
                    0);
            type.setLength(length);
        }
        else if ("smallint".equals(name)) {
            type = new TypeInteger(
                    "smallint", 
                    Types.SMALLINT, 
                    Integer.class, 
                    Value.TYPE_NUMBER, 
                    length,
                    32767, 
                    -32768); 
        }
        else if ("smallint unsigned".equals(name)) {
            type = new TypeInteger(
                    "smallint unsigned", 
                    Types.SMALLINT, 
                    Integer.class, 
                    Value.TYPE_NUMBER, 
                    length,
                    0, 
                    65535); 
        }
        else if ("mediumint".equals(name)) {
            type = new TypeInteger(
                    "mediumint", 
                    Types.INTEGER, 
                    Integer.class, 
                    Value.TYPE_NUMBER, 
                    length,
                    8388607, 
                    -8388608); 
        }
        else if ("mediumint unsigned".equals(name)) {
            type = new TypeInteger(
                    "mediumint unsigned", 
                    Types.INTEGER, 
                    Integer.class, 
                    Value.TYPE_NUMBER, 
                    length,
                    16777215,
                    0); 
        }
        else if ("int".equals(name) || "integer".equals(name)) {
            type = new TypeInteger(
                    "int", 
                    Types.INTEGER, 
                    Integer.class, 
                    Value.TYPE_NUMBER, 
                    length,
                    Integer.MAX_VALUE, 
                    Integer.MIN_VALUE + 1); 
            type.setLength(length);
        }
        else if ("int unsigned".equals(name) || "integer unsigned".equals(name)) {
            type = new TypeInteger(
                    "int unsigned", 
                    Types.BIGINT, 
                    Long.class, 
                    Value.TYPE_NUMBER, 
                    length,
                    4294967295l,
                    0); 
            type.setLength(length);
        }
        else if ("bigint".equals(name)) {
            type = new TypeInteger(
                    "bigint", 
                    Types.BIGINT, 
                    Long.class, 
                    Value.TYPE_NUMBER, 
                    length,
                    Long.MAX_VALUE, 
                    Long.MIN_VALUE + 1); 
        }
        else if ("bigint unsigned".equals(name)) {
            type = new TypeInteger(
                    "bigint unsigned", 
                    Types.BIGINT, 
                    Long.class, 
                    Value.TYPE_NUMBER, 
                    length,
                    Long.MAX_VALUE, 
                    Long.MIN_VALUE + 1); 
        }
        else if ("date".equals(name)) {
            type = new TypeDate("date");
        }
        else if ("enum".equals(name)) {
            type = new TypeString("enum", length, Types.VARCHAR);
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