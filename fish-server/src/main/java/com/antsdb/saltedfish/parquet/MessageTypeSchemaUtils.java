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

import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.MessageTypeBuilder;
import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.nosql.HColumnRow;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author Frank Li<lizc@tg-hd.com>
 */
public class MessageTypeSchemaUtils {
    final static Logger _log = UberUtil.getThisLogger();
    
    private static int DECIMAL_PRECESION = 18;
    private static int FIXED_LEN_DECIMAL_PRECESION = 38;
    private static int DECIMAL_SCALE = 5;
    public static int FIXED_LEN = 16;
    
    //private Map<String, MessageType> schemas = new ConcurrentHashMap<>();

    // INT64, INT32, BOOLEAN, BINARY, FLOAT, DOUBLE, INT96, FIXED_LEN_BYTE_ARRAY
    // all intgers -> int64, boolean -> boolean, time/date/timestamp -> int64, float/double -> double, string/clob ->
    // binary, binary/blob -> binary
    
    public MessageTypeSchemaUtils(String dbname) {}
    
    /**
      BOOLEAN: Bit Packed, LSB first
    INT32: 4 bytes 小端存储
    INT64: 8 bytes 小端存储
    INT96: 12 bytes 小端存储
    FLOAT: 4 bytes IEEE 小端存储
    DOUBLE: 8 bytes IEEE 小端存储
    BYTE_ARRAY: 4 bytes 小端存储字符数组长度，接着是字符字节
    FIXED_LEN_BYTE_ARRAY: 纯字符字节（这个应该是固定长度的字节）
     * @param ansdbType
     * @return
     */
    public static PrimitiveTypeName antsdbType2Parquet(int ansdbType) {
        PrimitiveTypeName name = null;
        switch (ansdbType) {
            case Value.TYPE_NULL:
                // case Value.FORMAT_NULL :
                name = PrimitiveTypeName.BINARY;
                break;
            case Value.TYPE_NUMBER:
            case Value.FORMAT_INT4:
                name = PrimitiveTypeName.INT64;
                break;
            case Value.FORMAT_INT8:
                name = PrimitiveTypeName.INT64;
                break;
            case Value.FORMAT_BIGINT:
                name = PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;//FIXED_LEN_BYTE_ARRAY
                break;
            case Value.FORMAT_FAST_DECIMAL:
            case Value.FORMAT_DECIMAL:
                name = PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
                break;
            case Value.TYPE_STRING:
            case Value.FORMAT_UTF8:
            case Value.FORMAT_UNICODE16:
                name = PrimitiveTypeName.BINARY;// .FIXED_LEN_BYTE_ARRAY;
                break;
            case Value.TYPE_BOOL:
                // case Value.FORMAT_BOOL:
                name = PrimitiveTypeName.BOOLEAN;
                break;
            case Value.TYPE_DATE:
            case Value.TYPE_TIME:
            case Value.TYPE_TIMESTAMP:
                // case Value. FORMAT_TIME:
                // case Value. FORMAT_DATE:
                // case Value. FORMAT_TIMESTAMP:
                name = PrimitiveTypeName.INT64;
                break;
            case Value.FORMAT_FLOAT4:
                name = PrimitiveTypeName.FLOAT;
                break;
            case Value.FORMAT_FLOAT8:
                name = PrimitiveTypeName.DOUBLE;
                break;
            case Value.TYPE_BYTES:
            case Value.TYPE_CLOB:
            case Value.TYPE_BLOB:
            case Value.TYPE_UNKNOWN:
            case Value.FORMAT_RECORD:

                // case Value.FORMAT_BYTES :
            case Value.FORMAT_KEY_BYTES:
                // case Value.FORMAT_BOUNDARY :
            case Value.FORMAT_INT4_ARRAY:
            case Value.FORMAT_CLOB_REF:
            case Value.FORMAT_BLOB_REF:
            case Value.FORMAT_ROW:
                name = PrimitiveTypeName.BINARY;// .FIXED_LEN_BYTE_ARRAY;
                break;

            default:
                name = PrimitiveTypeName.BINARY;
        }
        return name;
    }
    
    static OriginalType antsdbType2ParcateOriginalType(int ansdbType) {
        OriginalType originalType = null;
        switch (ansdbType) {
            case Value.TYPE_NULL:
                originalType = OriginalType.UTF8;
                break;
            case Value.TYPE_NUMBER:
            case Value.FORMAT_INT4:
                originalType = OriginalType.INT_32;
                break;
            case Value.FORMAT_INT8:
                originalType = OriginalType.INT_64;
                break;
            case Value.FORMAT_BIGINT:
                originalType = OriginalType.DECIMAL;
                break;
            case Value.FORMAT_FAST_DECIMAL:
            case Value.FORMAT_DECIMAL:
                originalType = OriginalType.DECIMAL;
                break;
            case Value.TYPE_STRING:
            case Value.FORMAT_UTF8:
            case Value.FORMAT_UNICODE16:
                originalType = OriginalType.UTF8;
                break;
            case Value.TYPE_DATE:
                originalType = OriginalType.DATE;
                break;
            case Value.TYPE_TIME:
                originalType = OriginalType.TIME_MILLIS;
                break;
            case Value.TYPE_TIMESTAMP:
                originalType = OriginalType.TIMESTAMP_MILLIS;
                break;
            case Value.FORMAT_FLOAT4:
                originalType = OriginalType.DECIMAL;
                break;
            case Value.FORMAT_FLOAT8:
                originalType = OriginalType.DECIMAL;
                break;
            case Value.TYPE_BYTES:
            case Value.TYPE_CLOB:
            case Value.TYPE_BLOB:
            case Value.TYPE_UNKNOWN:
            case Value.FORMAT_RECORD:
            case Value.FORMAT_KEY_BYTES:
            case Value.FORMAT_INT4_ARRAY:
            case Value.FORMAT_CLOB_REF:
            case Value.FORMAT_BLOB_REF:
            case Value.FORMAT_ROW:
                originalType = OriginalType.UTF8;
                break;
            default:
                originalType = OriginalType.UTF8;
        }
        return originalType;
    }
      
    public MessageType getSchema(TableName tableInfo,Mapping mapping,TableType tableType) {
        MessageType genschema = null;
        if(tableInfo.getTableName().startsWith("x") && tableInfo.getTableId()< 0x100) {
            genschema = getSystemSchema(tableInfo);
            if(genschema != null) {
                return genschema;
            }
        }
       
        String tableName = tableInfo.getDatabaseTableNameAndId();

        MessageTypeBuilder builder = Types.buildMessage();
        builder.required(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(Helper.SYS_COLUMN_PARQUETKEY_BYTES);
        if (tableType == TableType.INDEX && (mapping == null || mapping.getQualifiers() == null)) {
            _log.info("table:{} is index table,mapping is empty",tableName);
            return defaultSchema(tableName);
        }
        if (mapping != null && mapping.getQualifiers() != null) {
            List<Column> columns = mapping.getQualifiersByLists();
 
            if (!columns.contains(Column.valueOf(Helper.SYS_COLUMN_ROWID_BYTES, (int) Value.TYPE_BYTES))) {
                builder.required(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(Helper.SYS_COLUMN_ROWID_BYTES);
            }
                
            for (Column columnObj : columns) {
                if (columnObj == null) {
                    continue;
                }
                int anstdbType = columnObj.getType();
                String column = columnObj.getName();
                builder(builder,anstdbType,column);
            }
            if (!columns.contains(Column.valueOf(Helper.SYS_COLUMN_DATATYPE_BYTES, (int) Value.TYPE_BYTES))) {
                builder.optional(PrimitiveTypeName.BINARY).as(OriginalType.UTF8)
                                 .named(Helper.SYS_COLUMN_DATATYPE_BYTES);
            }
            if (!columns.contains(Column.valueOf(Helper.SYS_COLUMN_HASH_BYTES, (int) Value.TYPE_BYTES))) {
                builder.optional(PrimitiveTypeName.INT64).as(OriginalType.INT_64).named(Helper.SYS_COLUMN_HASH_BYTES);
            }
            if (!columns.contains(Column.valueOf(Helper.SYS_COLUMN_SIZE_BYTES, (int) Value.TYPE_BYTES))) {
                builder.optional(PrimitiveTypeName.INT32).as(OriginalType.INT_32).named(Helper.SYS_COLUMN_SIZE_BYTES);
            }
            if (!columns.contains(Column.valueOf(Helper.SYS_COLUMN_MISC_BYTES, (int) Value.TYPE_BYTES))) {
                builder.optional(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(Helper.SYS_COLUMN_MISC_BYTES);
            }
            if (!columns.contains(Column.valueOf(Helper.SYS_COLUMN_INDEXKEY_BYTES, (int) Value.TYPE_BYTES))) {
                builder.optional(PrimitiveTypeName.BINARY).as(OriginalType.UTF8)
                                .named(Helper.SYS_COLUMN_INDEXKEY_BYTES);
            }
            if (!columns.contains(Column.valueOf(Helper.SYS_COLUMN_STATUS, (int) Value.TYPE_BYTES))) {
                builder.optional(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(Helper.SYS_COLUMN_STATUS);
            }
            if (!columns.contains(Column.valueOf(Helper.SYS_COLUMN_VERSION_BYTES, (int) Value.TYPE_BYTES))) {
                builder.optional(PrimitiveTypeName.INT64).as(OriginalType.INT_64)
                                .named(Helper.SYS_COLUMN_VERSION_BYTES);
            }
            genschema = builder.named(tableName);
        }
        return genschema;
    }
    
    private static  void builder( MessageTypeBuilder builder,int anstdbType,String columnName) {
        PrimitiveTypeName typeName = antsdbType2Parquet(anstdbType);
        
        OriginalType originalType = antsdbType2ParcateOriginalType(anstdbType);
        if (typeName == PrimitiveTypeName.BINARY) {
            if(anstdbType == Value.FORMAT_BIGINT) {
                builder.optional(typeName)
                .as(originalType)
                .precision(DECIMAL_PRECESION)
                .named(columnName);
            }
            else if (originalType == OriginalType.DECIMAL) {
                builder.optional(typeName)
                        .as(originalType)
                        .precision(DECIMAL_PRECESION)
                        .scale(DECIMAL_SCALE)
                        .named(columnName);
            }
            else {
                builder.optional(typeName).as(originalType).named(columnName);
            }
        }
        else  if (typeName == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
            if(anstdbType == Value.FORMAT_BIGINT) {
                builder.optional(typeName)
                    .as(originalType)
                    .length(FIXED_LEN)
                    .precision(FIXED_LEN_DECIMAL_PRECESION)
                    .named(columnName);
            }
            else {
                builder.optional(typeName)
                        .as(originalType)
                        .length(FIXED_LEN)
                        .precision(FIXED_LEN_DECIMAL_PRECESION)
                        .scale(DECIMAL_SCALE)
                        .named(columnName);
            }
        }
        else {
            builder.optional(typeName).named(columnName);
        }
    }
    
    public MessageType getSystemSchema(TableName tableInfo) {
        if(tableInfo.getTableName().startsWith("x") && tableInfo.getTableId()< 0x100) {
            String tableName = tableInfo.getDatabaseTableName();
            return  defaultSchema(tableName);
        }
        return null;
    }
    
    public static MessageType genSchema(TableName tableInfo, List<HColumnRow> columns) {
        if (columns == null || columns.size() == 0) {
            throw new OrcaObjectStoreException(tableInfo.getTableName() + " columns is empty");
        }
        String tableName = tableInfo.getDatabaseTableNameAndId();

        MessageTypeBuilder builder = Types.buildMessage();
        builder.required(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(Helper.SYS_COLUMN_PARQUETKEY_BYTES);

        builder.optional(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(Helper.SYS_COLUMN_ROWID_BYTES);
        MessageType genschema = null;
        for (HColumnRow columnObj : columns) {
            if (columnObj == null || columnObj.isDeleted()) {
                continue;
            }
            String columnName = columnObj.getColumnName();
            int anstdbType = columnObj.getType();
            
            builder(builder,anstdbType,columnName);
            
        }

        builder.optional(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(Helper.SYS_COLUMN_DATATYPE_BYTES);
        builder.optional(PrimitiveTypeName.INT64).as(OriginalType.INT_64).named(Helper.SYS_COLUMN_HASH_BYTES);
        builder.optional(PrimitiveTypeName.INT32).as(OriginalType.INT_32).named(Helper.SYS_COLUMN_SIZE_BYTES);
        builder.optional(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(Helper.SYS_COLUMN_MISC_BYTES);
        builder.optional(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(Helper.SYS_COLUMN_INDEXKEY_BYTES);
        builder.optional(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(Helper.SYS_COLUMN_STATUS);
        builder.optional(PrimitiveTypeName.INT64).as(OriginalType.INT_64).named(Helper.SYS_COLUMN_VERSION_BYTES);
        
        genschema = builder.named(tableName);
        return genschema;
    }


    private static MessageType defaultSchema(String tableName) {
        MessageTypeBuilder builder = Types.buildMessage();
        builder.required(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(Helper.SYS_COLUMN_PARQUETKEY_BYTES);

        for (Column columnObj : Mapping.SYSTEM_QUALIFIERS) {
            builder.optional(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(columnObj.getName());
        }

        builder.optional(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(Helper.SYS_COLUMN_MISC_BYTES)
                .optional(PrimitiveTypeName.INT64).as(OriginalType.INT_64).named(Helper.SYS_COLUMN_HASH_BYTES)
                .optional(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(Helper.SYS_COLUMN_INDEXKEY_BYTES)
                .optional(PrimitiveTypeName.INT32).as(OriginalType.INT_32).named(Helper.SYS_COLUMN_SIZE_BYTES)
                .optional(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(Helper.SYS_COLUMN_DATATYPE_BYTES)
                .optional(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(Helper.SYS_COLUMN_ROWID_BYTES)
                .optional(PrimitiveTypeName.INT64).as(OriginalType.INT_64).named(Helper.SYS_COLUMN_VERSION_BYTES)
                .optional(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(Helper.SYS_COLUMN_STATUS)
        ;

        MessageType genschema = builder.named(tableName);
        return genschema;
    }
 

    public static MessageType addFiledMessageType(MessageType schema, String fieldName, int type) {
        if (schema == null) {
            return null;
        }
        if (fieldName == null || fieldName.length()==0) {
            return schema;
        }

        List<Type> fields = schema.getFields();
        for (Type field : fields) {
            String columnName = field.getName();
            if (fieldName.equalsIgnoreCase(columnName)) {
                return schema;
            }
        }
        PrimitiveTypeName typeName = antsdbType2Parquet(type);
        MessageTypeBuilder builder = Types.buildMessage();
        if (typeName == PrimitiveTypeName.BINARY) {
            builder.optional(typeName).as(OriginalType.UTF8).named(fieldName);
        }
        else {
            builder.optional(typeName).named(fieldName);
        }
        MessageType toMerge = builder.named(schema.getName());
        MessageType newSchema = schema.union(toMerge);
        return newSchema;
    }

    public static boolean equalsMessageType(MessageType schema, MessageType otherSchema) {
        if (schema == null && otherSchema == null) {
            return true;
        }
        else if (schema == null || otherSchema == null) {
            return false;
        }

        if (schema.equals(otherSchema)) {
            return true;
        }
        else {
            List<Type> fields = schema.getFields();
            List<Type> otherFields = otherSchema.getFields();
            if (fields == null && otherFields == null) {
                return true;
            }
            else if (fields == null || otherFields == null) {
                return false;
            }
            else if (fields.size() == 0 && otherFields.size() == 0) {
                return true;
            }
            else if (fields.size() == 0 || otherFields.size() == 0) {
                return false;
            }
            else if (fields.size() >= otherFields.size()) {
                for (Type field : fields) {
                    int index = fields.indexOf(field);
                    Type oterhField = otherFields.get(index);
                    if (oterhField.equals(field)) {
                        continue;
                    }
                    else {
                        String columnName = field.getName();
                        _log.info("otherSchema not contains field:{}({}),start schema merge...",columnName,oterhField.getName());
                        return false;
                    }
                }
            }
            else {
                for (Type field : otherFields) {
                    String columnName = field.getName();
                    if (schema.containsField(columnName)) {
                        continue;
                    }
                    else {
                        _log.info("schema not containsField:{}",columnName);
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public static MessageType removeFieldMessageType(MessageType schema, String fieldName) {
        if (fieldName == null || fieldName.length() == 0) {
            return schema;
        }
        if (schema == null) {
            return schema;
        }
        List<Type> fields = schema.getFields();
        List<Type> newFields = new ArrayList<>();
        for (Type field : fields) {
            String columnName = field.getName();
            if (!fieldName.equalsIgnoreCase(columnName)) {
                newFields.add(field);
            }
        }
        MessageType newSchema = new MessageType(schema.getName(), newFields);
        return newSchema;
    }

    public static Group copyGroupByMessageType(Group src, MessageType schema) {
        if (schema == null) {
            return src;
        }
        GroupFactory factory = new SimpleGroupFactory(schema);
        Group group = factory.newGroup();
        List<Type> fields = schema.getFields();
        for (Type field : fields) {
            String columnName = field.getName();
            //if source data not contains field continue;
            if(!src.getType().containsField(columnName)) {
                continue;
            }
            int count = src.getFieldRepetitionCount(columnName);
            if (count > 0) {
                PrimitiveType.PrimitiveTypeName typeStr = src.getType().getType(columnName).asPrimitiveType()
                        .getPrimitiveTypeName();
                if (typeStr == PrimitiveType.PrimitiveTypeName.BINARY) {
                    Binary binary = src.getBinary(columnName, 0);
                    if(binary != null) {
                        group.append(columnName,Binary.fromReusedByteArray(binary.getBytes()) );
                    }
                }
                else if (typeStr == PrimitiveType.PrimitiveTypeName.INT96) {
                    long data = src.getLong(columnName, 0);
                    group.append(columnName,data);
                }
                else if (typeStr == PrimitiveType.PrimitiveTypeName.INT64) {
                    long data = src.getLong(columnName, 0);
                    group.append(columnName,Long.parseLong(data+""));
                }
                else if (typeStr == PrimitiveType.PrimitiveTypeName.BOOLEAN) {
                    group.append(columnName, src.getBoolean(columnName, 0));
                }
                else if (typeStr == PrimitiveType.PrimitiveTypeName.INT32) {
                    group.append(columnName, Integer.valueOf(src.getInteger(columnName, 0)));
                }
                else if (typeStr == PrimitiveType.PrimitiveTypeName.DOUBLE) {
                    group.append(columnName, src.getDouble(columnName, 0));
                }
                else if (typeStr == PrimitiveType.PrimitiveTypeName.FLOAT) {
                    group.append(columnName, src.getFloat(columnName, 0));
                }
                else {
                    throw new OrcaObjectStoreException(columnName+"\t typeStr:"+typeStr+" type error");
                }
            }
        }
        return group;
    }

    public static MessageType genNewSchema(TableName tableInfo, String fieldName, int type) {
        String tableName = tableInfo.getDatabaseTableNameAndId();
        MessageTypeBuilder builder = Types.buildMessage();

        builder.required(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(Helper.SYS_COLUMN_PARQUETKEY_BYTES)
                .optional(PrimitiveTypeName.INT64).as(OriginalType.INT_64).named(Helper.SYS_COLUMN_HASH_BYTES)
                .optional(PrimitiveTypeName.INT32).as(OriginalType.INT_32).named(Helper.SYS_COLUMN_SIZE_BYTES)
                .optional(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(Helper.SYS_COLUMN_DATATYPE_BYTES)
                .optional(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(Helper.SYS_COLUMN_ROWID_BYTES)
                .optional(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(Helper.SYS_COLUMN_MISC_BYTES)
                .optional(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(Helper.SYS_COLUMN_INDEXKEY_BYTES)
                .optional(PrimitiveTypeName.INT64).as(OriginalType.INT_64).named(Helper.SYS_COLUMN_VERSION_BYTES)
                .optional(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(Helper.SYS_COLUMN_STATUS)
        ;
        
        builder(builder,type,fieldName);
        
        MessageType toMerge = builder.named(tableName);
        return toMerge;
    }
}
