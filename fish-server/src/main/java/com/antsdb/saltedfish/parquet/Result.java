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
package com.antsdb.saltedfish.parquet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author Frank Li<lizc@tg-hd.com>
 */
public class Result {
    final static Logger _log = UberUtil.getThisLogger();
    private Group group;
    private MessageType schema;
    private TableName tableInfo;

    public Result() {}

    public Result(Group group, MessageType schema, TableName tableName) {
        this.group = group;
        this.schema = schema;
        this.tableInfo = tableName;
    }

    public Group getGroup() {
        return group;
    }

    public void setGroup(Group group) {
        this.group = group;
    }

    public boolean isEmpty() {
        return group == null;
    }

    public Long getLongVal(String string) {
        if (group == null) {
            return 0L;
        }
        int fieldCount = group.getFieldRepetitionCount(string);
        if (fieldCount > 0) {
            return group.getLong(string, 0);
        }
        return null;
    }

    public String getStringVal(String string) {
        if (group == null) {
            return "";
        }
        int fieldCount = group.getFieldRepetitionCount(string);
        if (fieldCount > 0) {
            return group.getString(string, 0);
        }
        else {
            return null;
        }
    }

    public boolean getBooleanVal(String string) {
        if (group == null) {
            return false;
        }
        int fieldCount = group.getFieldRepetitionCount(string);
        if (fieldCount > 0) {
            return group.getBoolean(string, 0);
        }
        return false;
    }

    public byte[] getRowKey() {
        return group.getBinary(Helper.SYS_COLUMN_PARQUETKEY_BYTES, 0).getBytes();
    }

    public Map<String, Object> toMap() {
        Map<String, Object> row = new HashMap<>();
        List<Type> fields = schema.getFields();
        for (Type field : fields) {
            String columnName = field.getName();
            PrimitiveType primitiveType = field.asPrimitiveType();
            _log.info("{} ,field {} primitiveType is {}", tableInfo.getDatabaseTableName(), columnName, primitiveType.getName());
            int fieldCount = group.getFieldRepetitionCount(columnName);
            Object columnVal = null;
            if (fieldCount > 0) {
                String typeStr = group.getType().getType(columnName).toString();
                if (typeStr.contains("binary")) {
                    Binary binaryVal = group.getBinary(columnName, 0);
                    if (binaryVal != null) {
                        columnVal = binaryVal.getBytes();
                    } 
                }
                else if (typeStr.contains("int64")) {
                    columnVal = group.getLong(columnName, 0);
                }
                else if (typeStr.contains(PrimitiveType.PrimitiveTypeName.BOOLEAN.name().toLowerCase())) {
                    columnVal = group.getBoolean(columnName, 0) ;
                }
                else if (typeStr.contains(PrimitiveType.PrimitiveTypeName.INT32.name().toLowerCase())) {
                    columnVal = group.getInteger(columnName, 0);
                }
                else if (typeStr.contains(PrimitiveType.PrimitiveTypeName.INT96.name().toLowerCase())) {
                    Binary binaryVal = group.getBinary(columnName, 0);
                    if (binaryVal != null) {
                        columnVal = binaryVal.getBytes();
                    }
                }
                else if (typeStr.contains(PrimitiveType.PrimitiveTypeName.FLOAT.name().toLowerCase())) {
                    columnVal = group.getFloat(columnName, 0);
                }
                else if (typeStr.contains(PrimitiveType.PrimitiveTypeName.DOUBLE.name().toLowerCase())) {
                    columnVal = group.getDouble(columnName, 0);
                }
                else {
                    _log.warn("data type:{},columnName:{} NotImplementedException",typeStr,columnName);
                    throw new NotImplementedException();
                }
            }
            row.put(columnName, columnVal);
        }
        return row;
    }

    
    public byte[] getBytesVal(String cloumnName) {
        int fieldCount = group.getFieldRepetitionCount(cloumnName);
        if (fieldCount > 0) {
            return group.getBinary(cloumnName, 0).getBytes();
        }
        else {
            return null;
        }
    }
}
