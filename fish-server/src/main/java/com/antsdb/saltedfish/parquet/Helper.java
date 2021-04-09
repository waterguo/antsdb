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

import static java.lang.Math.pow;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.codec.Charsets;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.BigInt;
import com.antsdb.saltedfish.cpp.FastDecimal;
import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.FishUtf8;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Int4Array;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.cpp.Unicode16;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.nosql.IndexLine;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.nosql.VaporizingRow;
import com.antsdb.saltedfish.obs.ObsProvider;
import com.antsdb.saltedfish.obs.cache.ObsCache;
import com.antsdb.saltedfish.obs.cache.ObsFileReference;
import com.antsdb.saltedfish.parquet.bean.Partition;
import com.antsdb.saltedfish.parquet.merge.MergerUtils;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.BlobReference;
import com.antsdb.saltedfish.sql.vdm.KeyMaker;
import com.antsdb.saltedfish.storage.OrcaHBaseException;
import com.antsdb.saltedfish.util.UberUtil;
/**
 * hdfs helper functions but in a more user friendly way
 * 
 * @author Frank Li<lizc@tg-hd.com>
 */

public final class Helper {
    final static Logger _log = UberUtil.getThisLogger();

    public static final String SYS_COLUMN_ROWID_BYTES = "*rowid";
    public static final String SYS_COLUMN_INDEXKEY_BYTES = "*key";
    
    public static final String SYS_COLUMN_DATATYPE_BYTES = "*type";
    public static final String SYS_COLUMN_VERSION_BYTES = "*version";//long
    public static final String SYS_COLUMN_MISC_BYTES = "*misc";
    public static final String SYS_COLUMN_SIZE_BYTES = "*size";
    public static final String SYS_COLUMN_HASH_BYTES = "*hash";//long
    public static final String SYS_COLUMN_STATUS = "*status";// value 0:default insert ;1,update,2,delete
    public static final String SYS_COLUMN_PARQUETKEY_BYTES = "*rowkey";

    public static final byte[] SYS_COLUMN_STATUS_INSERT = Bytes.toBytes(0);
    public static final byte[] SYS_COLUMN_STATUS_UPDATE = Bytes.toBytes(1);
    public static final byte[] SYS_COLUMN_STATUS_DELETE = Bytes.toBytes(2);

    public static String getKeyName(byte[] key) {
        if ((key.length == 2) && (key[0] < 0x30)) {
            int column = key[0] << 8 | key[1];
            return Integer.toString(column);
        }
        return Bytes.toString(key);
    }

    public static boolean existsNamespace(ObsProvider provider,String remoteDataHome, String nameSpace) {
        try {
            String path = remoteDataHome + nameSpace;
            boolean exists = provider.existDirectory(path);
            return exists;
        }
        catch (Exception ex) {
            throw new OrcaObjectStoreException(ex);
        }
    }

    public static boolean existsTable( ObsProvider provider,String remoteDataHome,TableName tableInfo) {
        if (tableInfo == null) {
            return false;
        }
        try {
            String path = remoteDataHome + tableInfo.getTablePath();
            boolean exists = provider.existDirectory(path);
            
            return exists;
        }
        catch (Exception ex) {
            throw new OrcaObjectStoreException(ex, "Failed to check existence of table {}.{}", tableInfo);
        }
    }

    public static boolean existsTable(ObsProvider provider,String remoteDataHome, String ns, String name, int tableId) {
        return existsTable(provider,remoteDataHome, TableName.valueOf(ns, name, tableId));
    }

    public static void createNamespace(ObsProvider provider,String remoteDataHome, String namespace) {
        try {
            if (!Helper.existsNamespace( provider, remoteDataHome, namespace)) {
                String path = remoteDataHome + namespace;
                provider.createDirectory(path);
            }
        }
        catch (Exception ex) {
            throw new OrcaObjectStoreException(ex, "Failed to create namespace - " + namespace);
        }
    }

    public static void createTable(PartitionIndex partitionIndex,
            ObsProvider provider,
            File localDataHome,
            String remoteDataHome,
            TableName tableInfo) {
        _log.info(" create table :{}", tableInfo);
        if (!Helper.existsTable(provider,remoteDataHome, tableInfo)) {
            // Create namespace first
            createNamespace(provider,remoteDataHome, tableInfo.getDatabaseName());
            // Create table
            try {
                
                File tableLocalPath = new File(localDataHome,tableInfo.getTablePath());
                boolean localResult = tableLocalPath.mkdirs();
                
                String tableRemotePath = remoteDataHome + tableInfo.getTablePath();
                provider.createDirectory(tableRemotePath);
                _log.debug("create table {},localResult:{}", tableInfo.toString(), localResult);
                
                // create partition index
                Partition index = new Partition(
                        tableInfo.getDatabaseName(), 
                        tableInfo.getTableName(),
                        tableInfo.getTableId());
                
                index.setId(partitionIndex.getNextIndexId());
                index.setStartKey(Partition.defaultStartKey);
                index.setEndKey(Partition.defaultEndKey);
                index.setStatus(PartitionIndex.STATUS_NODATA);
                index.setRowCount(0L);
                index.setDataSize(0L);
                partitionIndex.add(index);
            }
            catch (Exception ex) {
                _log.error(ex.getMessage(), ex);
                throw new OrcaObjectStoreException("Failed to create table - " + tableInfo.getTableNameAndId(), ex);
            }
        }
    }

    public static Result get(Partition partition,
            ObsCache obsCache, 
            TableName tableName, 
            byte[] key
            ) throws Exception {
        Result r = readRowByIndex(obsCache,tableName,partition, key,TableType.DATA);
        return r;
    }
    
    public static Result getIndex(Partition partition,
            ObsCache obsCache, 
            TableName tableName, 
            byte[] key
            ) throws Exception {
        Result r = readRowByIndex(obsCache,tableName,partition, key,TableType.INDEX);
        return r;
    }
    
    
    private static Result readRowByIndex(ObsCache obsCache,
            TableName tableName, 
            Partition index, 
            byte[] key,
            TableType tableType) throws Exception {
        Group hit = null;
        MessageType schema = null;
        if(index != null) {
            String objectKey =  tableName.getTablePath() + index.getVersionFileName();
            ObsFileReference parquetFile = obsCache.get( objectKey);
            if(parquetFile!=null) {
                long fileSize = parquetFile.getFsize();
                if (fileSize > ParquetDataWriter.minParquetSize) {
                    Path path = new Path(parquetFile.getFile().getAbsolutePath());
                    try (ParquetDataReader reader = new ParquetDataReader(path, new Configuration());) {
                        Group group = null;
                        while ((group = reader.read()) != null) {
                            byte[] tmpKey = MergerUtils.getKeyByGroup(group, tableType);
                            if (Bytes.equals(tmpKey, key)) {
                                hit = group;
                                if (hit != null && schema == null) {
                                    schema = getSchemaByFile(parquetFile.getFile(), new Configuration());
                                }
                                break;// skip while
                            }
                        }
                    }
                    catch (Exception e) {
                        _log.error(e.getMessage(), e);
                        throw new OrcaObjectStoreException(e);
                    }
                }
            }
            else {
                _log.debug("table:{},data file :{} not exists,searchKey:{},startKey:{},endKey:{},count:{}",
                        tableName.getTableNameAndId(),
                        objectKey,
                        (key == null)? "searchkey is null":Bytes.toHex(key),
                        Bytes.toHex(index.getStartKey()),
                        Bytes.toHex(index.getEndKey()),
                        index.getRowCount()
                        );
            }
        }
        return new Result(hit, schema, tableName);
    }

    private static MessageType getSchemaByFile(File filePath, Configuration configuration) throws IOException {
        long fileSize = filePath.length();
        if (fileSize > ParquetDataWriter.minParquetSize) {
            ParquetFileReader fileReader = null;
            try {
                ParquetReadOptions readOption = HadoopReadOptions.builder(configuration).build();
                InputFile file = HadoopInputFile.fromPath(new Path(filePath.getAbsolutePath()), configuration);
                fileReader = ParquetFileReader.open(file, readOption);
                MessageType schema = fileReader.getFileMetaData().getSchema();
                return schema;
            }
            finally {
                if (fileReader != null) {
                    try {
                        fileReader.close();
                    }
                    catch (IOException e) {
                        _log.error(filePath + "\t" + e.getMessage(), e);
                    }
                }
            }
        }
        return null;
    }

    public static int getSize(Result r) {
        Group g = r.getGroup();
        int size = 0;
        int fieldCount = g.getFieldRepetitionCount(SYS_COLUMN_SIZE_BYTES);
        if (fieldCount > 0) {
            size = g.getInteger(SYS_COLUMN_SIZE_BYTES, 0);
        }
        if (size > 0) {
            return size;
        }
        else {
            byte[] rowKey = r.getRowKey();
            byte[] indexKey = r.getBytesVal(SYS_COLUMN_INDEXKEY_BYTES);
            if(indexKey == null) {
                indexKey = new byte[] {};
            }
            return indexKey.length + KeyBytes.HEADER_SIZE + rowKey.length + KeyBytes.HEADER_SIZE + 1;
        }
    }

    public static long toRow(Heap heap, Group group, TableMeta table, int tableId) {
        if (group == null) {
            return 0;
        }
        int fieldCount = group.getFieldRepetitionCount(SYS_COLUMN_DATATYPE_BYTES);
        byte[] colDataType = null;
        if (fieldCount > 0) {
            colDataType = group.getBinary(SYS_COLUMN_DATATYPE_BYTES, 0).getBytes();
        }
        if (colDataType == null) {
            _log.warn(table.getTableName() + " colDataType is empty");
            return 0;
        }
        int sizeFieldCount = group.getFieldRepetitionCount(SYS_COLUMN_SIZE_BYTES);
        int size = 0;
        if (sizeFieldCount > 0) {
            size = group.getInteger(SYS_COLUMN_SIZE_BYTES, 0);
        }
        long version = 1;
        int versionFieldCount = group.getFieldRepetitionCount(SYS_COLUMN_VERSION_BYTES);
        if (versionFieldCount > 0) {
            version =  group.getLong(SYS_COLUMN_VERSION_BYTES,0);
        }
        // populate the row. system table doesn't come with metadata
        VaporizingRow row = null;
        byte[] rowKey = group.getBinary(SYS_COLUMN_PARQUETKEY_BYTES, 0).getBytes();
        byte[] key = hdfsKeyToAnts(rowKey);
        if (table != null) {
            row = populateUsingMetadataByGroup(heap, table, group, colDataType, size, key);
        }
        else if (tableId < 0x100) {
            row = populateDirectByGroup(heap, group, colDataType, size, key);
        }
        else {
            throw new OrcaHBaseException("metadata not found for table " + tableId);
        }
        row.setVersion(version);
        long pRow = Row.from(heap, row);
        return pRow;
    }

    public static byte[] hdfsKeyToAnts(byte[] bytes) {
        KeyMaker.flipEndian(bytes);
        return bytes;
    }

    private static VaporizingRow populateDirectByGroup(Heap heap, Group data, byte[] types, int size, byte[] rowkey) {
        int maxColumnId = types.length - 1;
        VaporizingRow row = new VaporizingRow(heap, maxColumnId);
        row.setKey(rowkey);

        GroupType groupType = data.getType();
        List<Type> fields = groupType.getFields();

        for (Type field : fields) {
            String columnName = field.getName();
            byte[] value = null;
            if (columnName.startsWith("*")) {
                continue;
            }
            int fieldCount = data.getFieldRepetitionCount(columnName);
            if (fieldCount <= 0) {
                //_log.trace("to row columnName fieldCount is 0 continue columnName={},fieldCount={}",columnName,fieldCount);
                continue;
            }
            int column = Integer.parseInt(columnName.substring(1));
            column = column + 1;
            if (column >= types.length || types[column] == Value.FORMAT_NULL) {
                //_log.trace("to row columnName : {},types[column] is null :{}",columnName,types[column]);
                continue;
            }
            PrimitiveType.PrimitiveTypeName typeStr = data.getType()
                        .getType(columnName)
                        .asPrimitiveType()
                        .getPrimitiveTypeName();
            if (typeStr == PrimitiveType.PrimitiveTypeName.BINARY) {
                Binary binaryVal = data.getBinary(columnName, 0);
                if (binaryVal != null) {
                    value = binaryVal.getBytes();
                    long pValue = toMemory(heap, types[column], value, row.getKeyAddress());
                    row.setFieldAddress(column, pValue);
                }
            }
            else if (typeStr == PrimitiveType.PrimitiveTypeName.INT64) {
                Long colValue = data.getLong(columnName, 0);
                byte[] colValueBytes = Bytes.toBytes(colValue);
                long pValue = toMemory(heap, types[column], colValueBytes, row.getKeyAddress());
                row.setFieldAddress(column, pValue);
            }
            else if (typeStr == PrimitiveType.PrimitiveTypeName.INT32) {
                Integer colValue = data.getInteger(columnName, 0);
                byte[] colValueBytes = Bytes.toBytes(colValue);
                long pValue = toMemory(heap, types[column], colValueBytes, row.getKeyAddress());
                row.setFieldAddress(column, pValue);
            }
            else if (typeStr == PrimitiveType.PrimitiveTypeName.BOOLEAN) {
                Boolean colValue = data.getBoolean(columnName, 0);
                byte[] colValueBytes = Bytes.toBytes(colValue);
                long pValue = toMemory(heap, types[column], colValueBytes, row.getKeyAddress());
                row.setFieldAddress(column, pValue);
            }
            else if (typeStr == PrimitiveType.PrimitiveTypeName.DOUBLE) {
                Double colValue = data.getDouble(columnName, 0);
                byte[] colValueBytes = Bytes.toBytes(colValue);
                long pValue = toMemory(heap, types[column], colValueBytes, row.getKeyAddress());
                row.setFieldAddress(column, pValue);
            }
            else if (typeStr == PrimitiveType.PrimitiveTypeName.FLOAT) {
                Float colValue = data.getFloat(columnName, 0);
                byte[] colValueBytes = Bytes.toBytes(colValue);
                long pValue = toMemory(heap, types[column], colValueBytes, row.getKeyAddress());
                row.setFieldAddress(column, pValue);
            }
            else {
                throw new NotImplementedException();
            }
        }
        return row;
    }

    private static VaporizingRow populateUsingMetadataByGroup(Heap heap, TableMeta table, Group data, byte[] types,
            int size, byte[] rowkey) {
        int maxColumnId = types.length - 1;
        VaporizingRow row = new VaporizingRow(heap, maxColumnId);
        row.setKey(rowkey);
        for (int i = 0; i <= maxColumnId; i++) {
            if (i == 0) {
                // rowid
                byte[] colValueBytes = null;
                int fieldCount = data.getFieldRepetitionCount(SYS_COLUMN_ROWID_BYTES);
                if (fieldCount <= 0) {
                    continue;
                }
                colValueBytes = data.getBinary(SYS_COLUMN_ROWID_BYTES, 0).getBytes();
                long pValue = toMemory(heap, Value.FORMAT_INT8, colValueBytes, row.getKeyAddress());
                row.setFieldAddress(i, pValue);
                continue;
            }
            ColumnMeta colMeta;
            colMeta = table.getColumnByColumnId(i);
            // skip invalid column - not found column meta
            if (colMeta == null) {
                continue;
            }
            // skip non-existing column
            if (i >= types.length) {
                break;
            }
            // Add column
            // Get column name
            String colName = colMeta.getColumnName();
            boolean filedExist = data.getType().containsField(colName);
            if(!filedExist) {
                _log.warn("table:{},cloumn {} not exist.",table.getTableName(),colName);
                continue;
            }
            int fieldCount = data.getFieldRepetitionCount(colName);
            if (fieldCount > 0) {
                PrimitiveType.PrimitiveTypeName typeStr = data.getType()
                        .getType(colName)
                        .asPrimitiveType()
                        .getPrimitiveTypeName();
                if (typeStr == PrimitiveType.PrimitiveTypeName.BINARY) {
                    Binary binaryVal = data.getBinary(colName, 0); 
                    if (binaryVal != null) {
                        byte[] colValueBytes = binaryVal.getBytes();
                        
                        OriginalType originalType = data.getType().getType(colName).getOriginalType();
                        if (originalType == OriginalType.DECIMAL) {
                            long pValue = toMemory(heap, types[i], colValueBytes, row.getKeyAddress());
                            row.setFieldAddress(i, pValue);
                        }
                        else {
                            long pValue = toMemory(heap, types[i], colValueBytes, row.getKeyAddress());
                            row.setFieldAddress(i, pValue);
                        }
                    }
                }
                else if (typeStr == PrimitiveType.PrimitiveTypeName.INT96) {
                    Long colValue = data.getLong(colName, 0);
                    byte[] colValueBytes = Bytes.toBytes(colValue);
                    long pValue = toMemory(heap, types[i], colValueBytes, row.getKeyAddress());
                    row.setFieldAddress(i, pValue);
                }
                else if (typeStr == PrimitiveType.PrimitiveTypeName.INT64) {
                    Long colValue = data.getLong(colName, 0);
                    byte[] colValueBytes = Bytes.toBytes(colValue);
                    long pValue = toMemory(heap, types[i], colValueBytes, row.getKeyAddress());
                    row.setFieldAddress(i, pValue);
                }
                else if (typeStr == PrimitiveType.PrimitiveTypeName.INT32) {
                    Integer colValue = data.getInteger(colName, 0);
                    byte[] colValueBytes = Bytes.toBytes(colValue);
                    long pValue = toMemory(heap, types[i], colValueBytes, row.getKeyAddress());
                    row.setFieldAddress(i, pValue);
                }
                else if (typeStr == PrimitiveType.PrimitiveTypeName.BOOLEAN) {
                    Boolean colValue = data.getBoolean(colName, 0);
                    byte[] colValueBytes = Bytes.toBytes(colValue);
                    long pValue = toMemory(heap, types[i], colValueBytes, row.getKeyAddress());
                    row.setFieldAddress(i, pValue);
                }
                else if (typeStr == PrimitiveType.PrimitiveTypeName.DOUBLE) {
                    Double colValue = data.getDouble(colName, 0);
                    byte[] colValueBytes = Bytes.toBytes(colValue);
                    long pValue = toMemory(heap, types[i], colValueBytes, row.getKeyAddress());
                    row.setFieldAddress(i, pValue);
                }
                else if (typeStr == PrimitiveType.PrimitiveTypeName.FLOAT) {
                    Float colValue = data.getFloat(colName, 0);
                    byte[] colValueBytes = Bytes.toBytes(colValue);
                    long pValue = toMemory(heap, types[i], colValueBytes, row.getKeyAddress());
                    row.setFieldAddress(i, pValue);
                }
                else if (typeStr == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
                   GroupType type = data.getType();
                   OriginalType originalType = type.getType(colName).getOriginalType();
                    if (originalType != null && originalType.name().equals("DECIMAL")){
                       
                        int precision = type.getType(colName).asPrimitiveType().getDecimalMetadata().getPrecision();
                        int scale = type.getType(colName).asPrimitiveType().getDecimalMetadata().getScale();
                        BigDecimal decimalValue = binaryToDecimal(data.getBinary(colName, 0),precision, scale);
                       
                        byte[] colValueBytes = Bytes.toBytes(decimalValue);
                        long pValue = toMemory(heap, types[i], colValueBytes, row.getKeyAddress());
                        row.setFieldAddress(i, pValue);
                    }
                    else {
                        Binary binaryVal = data.getBinary(colName, 0); 
                        if (binaryVal != null) {
                            byte[] colValueBytes = binaryVal.getBytes();
                            long pValue = toMemory(heap, types[i], colValueBytes, row.getKeyAddress());
                            row.setFieldAddress(i, pValue);
                        }
                    }
                }
                else {
                    throw new NotImplementedException();
                }
            }
        }
        return row;
    }

    public static byte getType(long pValue) {
        if (pValue == 0) {
            return Value.TYPE_NULL;
        }
        return Value.getFormat(null, pValue);
    }

    private static long putBytesReversed(Heap heap, int type, byte[] value) {
        long p = heap.alloc(value.length + 1);
        Unsafe.putByte(p, (byte) type);
        for (int i = 0; i < value.length; i++) {
            Unsafe.putByte(p + 1 + i, value[value.length - i - 1]);
        }
        return p;
    }

    public static long toMemory(Heap heap, int type, byte[] value, long pKey) {
        if (value == null) {
            return 0;
        }
        long pValue;
        if (type == Value.FORMAT_NULL) {
            return 0;
        }
        else if (type == Value.FORMAT_INT4) {
            pValue = putBytesReversed(heap, type, value);
        }
        else if (type == Value.FORMAT_INT8) {
            pValue = putBytesReversed(heap, type, value);
        }
        else if (type == Value.FORMAT_BIGINT) {
           BigInteger bd = new BigInteger(value);
           pValue = BigInt.allocSet(heap, bd);
        }
        else if (type == Value.FORMAT_FAST_DECIMAL) {
            BigDecimal bd = Bytes.toBigDecimal(value);
            pValue = FastDecimal.allocSet(heap, bd);
        }
        else if (type == Value.FORMAT_DECIMAL) {
            BigDecimal bd = Bytes.toBigDecimal(value);
            return FishObject.allocSet(heap, bd);
        }
        else if (type == Value.FORMAT_FLOAT4) {
            pValue = putBytesReversed(heap, type, value);
        }
        else if (type == Value.FORMAT_FLOAT8) {
            pValue = putBytesReversed(heap, type, value);
        }
        else if (type == Value.FORMAT_TIME) {
            pValue = putBytesReversed(heap, type, value);
        }
        else if (type == Value.FORMAT_DATE) {
            pValue = putBytesReversed(heap, type, value);
        }
        else if (type == Value.FORMAT_TIMESTAMP) {
            pValue = putBytesReversed(heap, type, value);
        }
        else if (type == Value.FORMAT_UTF8) {
            pValue = heap.alloc(value.length + FishUtf8.HEADER_SIZE);
            Unsafe.putByte(pValue, (byte) type);
            Unsafe.putInt3(pValue + 1, value.length);
            Unsafe.putBytes(pValue + FishUtf8.HEADER_SIZE, value);
        }
        else if (type == Value.FORMAT_UNICODE16) {
            String s = Bytes.toString(value);
            pValue = Unicode16.allocSet(heap, s);
        }
        else if (type == Value.FORMAT_BOOL) {
            pValue = putBytesReversed(heap, type, value);
        }
        else if (type == Value.FORMAT_BYTES) {
            pValue = com.antsdb.saltedfish.cpp.Bytes.allocSet(heap, value);
        }
        else if (type == Value.FORMAT_INT4_ARRAY) {
            pValue = Int4Array.alloc(heap, value).getAddress();
        }
        else if (type == Value.FORMAT_CLOB_REF) {
            int dataSize = Bytes.toInt(value);
            pValue = BlobReference.alloc(heap, pKey, dataSize).getAddress();
        }
        else if (type == Value.FORMAT_BLOB_REF) {
            int dataSize = Bytes.toInt(value);
            pValue = BlobReference.alloc(heap, pKey, dataSize).getAddress();
        }
        else {
            throw new IllegalArgumentException(String.valueOf(type));
        }
        return pValue;
    }

    public static byte[] toBytes(long pValue) {
        if (pValue == 0) {
            return null;
        }

        int length = 0;
        int offset = 1;

        // get data type
        byte dataType = Unsafe.getByte(pValue);
        boolean needInverse = true;
        if (dataType == Value.FORMAT_INT4) {
            length = 4;
        }
        else if (dataType == Value.FORMAT_INT8) {
            length = 8;
        }
        else if (dataType == Value.FORMAT_NULL) {
            length = 0;
        }
        else if (dataType == Value.FORMAT_BOOL) {
            length = 1;
        }
        else if (dataType == Value.FORMAT_FLOAT4) {
            length = 4;
        }
        else if (dataType == Value.FORMAT_FLOAT8) {
            length = 8;
        }
        else if (dataType == Value.FORMAT_DATE) {
            length = 8;
        }
        else if (dataType == Value.FORMAT_TIMESTAMP) {
            length = 8;
        }
        else if (dataType == Value.FORMAT_TIME) {
            length = 8;
        }
        else if (dataType == Value.FORMAT_DECIMAL) {
            BigDecimal bigDecimal = (BigDecimal) FishObject.get(null, pValue);
            return Bytes.toBytes(bigDecimal);
        }
        else if (dataType == Value.FORMAT_FAST_DECIMAL) {
            BigDecimal fastDecimal = (BigDecimal) FishObject.get(null, pValue);
            return Bytes.toBytes(fastDecimal);
        }
        else if (dataType == Value.FORMAT_UTF8) {
            return FishUtf8.getBytes(pValue);
        }
        else if (dataType == Value.FORMAT_UNICODE16) {
            String s = Unicode16.get(null, pValue);
            return s.getBytes(Charsets.UTF_8);
        }
        else if (dataType == Value.FORMAT_BYTES) {
            needInverse = false;
            length = Unsafe.getInt3(pValue + 1);
            offset = 4;
        }
        else if (dataType == Value.FORMAT_INT4_ARRAY) {
            return new Int4Array(pValue).toBytes();
        }
        else if (dataType == Value.FORMAT_BLOB_REF) {
            BlobReference ref = new BlobReference(pValue);
            return Bytes.toBytes(ref.getDataSize());
        }
        else {
            throw new IllegalArgumentException(String.valueOf(dataType));
        }

        byte[] bytes = new byte[length];
        Unsafe.getBytes(pValue + offset, bytes);
        if (needInverse) {
            for (int i = 0; i < bytes.length / 2; i++) {
                byte bt = bytes[i];
                bytes[i] = bytes[bytes.length - i - 1];
                bytes[bytes.length - i - 1] = bt;
            }
        }
        return bytes;
    }

    public static byte[] antsKeyToHdfs(long pkey) {
        byte[] bytes = KeyBytes.create(pkey).get();
        KeyMaker.flipEndian(bytes);
        return bytes;
    }

    public static long toIndexLine(Heap heap, Group group) {
        if (group == null) {
            return 0;
        }
        
        byte[] rowKey = group.getBinary(SYS_COLUMN_PARQUETKEY_BYTES, 0).getBytes();
        byte[] indexKey = group.getBinary(SYS_COLUMN_INDEXKEY_BYTES, 0).getBytes();
        long version = group.getLong(SYS_COLUMN_VERSION_BYTES, 0);
        
        String misc = null;
        int miscfieldCount = group.getFieldRepetitionCount(SYS_COLUMN_MISC_BYTES);
        if (miscfieldCount > 0) {
            misc = group.getString(SYS_COLUMN_MISC_BYTES, 0);
        }
        if (misc != null) {
            byte miscInt = Bytes.toBytes(misc)[0];
            indexKey = hdfsKeyToAnts(indexKey);
            rowKey = hdfsKeyToAnts(rowKey);
            return IndexLine.alloc(heap, version, indexKey, rowKey, miscInt).getAddress();
        }
        return -1;
    }
    
    
    public static BigDecimal binaryToDecimal(Binary value, int precision, int scale) {
        /*
         * Precision <= 18 checks for the max number of digits for an unscaled long, else treat with big integer
         * conversion
         */
        if (precision <= 18) {
            ByteBuffer buffer = value.toByteBuffer();
            byte[] bytes = buffer.array();
            int start = buffer.arrayOffset() + buffer.position();
            int end = buffer.arrayOffset() + buffer.limit();
            long unscaled = 0L;
            int i = start;
            while (i < end) {
                unscaled = (unscaled << 8 | bytes[i] & 0xff);
                i++;
            }
            int bits = 8 * (end - start);
            long unscaledNew = (unscaled << (64 - bits)) >> (64 - bits);
            if (unscaledNew <= -pow(10, 18) || unscaledNew >= pow(10, 18)) {
                return new BigDecimal(unscaledNew);
            }
            else {
                return BigDecimal.valueOf(unscaledNew / pow(10, scale));
            }
        }
        else {
            return new BigDecimal(new BigInteger(value.getBytes()), scale);
        }
    }
}
