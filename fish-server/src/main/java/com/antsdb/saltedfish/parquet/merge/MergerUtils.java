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
package com.antsdb.saltedfish.parquet.merge;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.parquet.Helper;
import com.antsdb.saltedfish.parquet.OrcaObjectStoreException;
import com.antsdb.saltedfish.parquet.PartitionIndex;
import com.antsdb.saltedfish.parquet.TableName;
import com.antsdb.saltedfish.parquet.bean.Partition;

public class MergerUtils {
    static Logger _logSyncLog = LoggerFactory.getLogger(PartitionMerger.class.getName() + ".sync-log");
    public static void updateEndKey(Partition index, Group data,TableType tableType) {
        if (data == null) {
            return;
        }
        byte[] partitionKey = getKeyByGroup(data,tableType);
        if (index.getEndKey() == null 
                || Bytes.compareTo(partitionKey, index.getEndKey()) > 0
                || (Bytes.compareTo(Partition.defaultEndKey, index.getEndKey()) == 0)) {
            index.setEndKey(partitionKey);
        }
    }
    
    public static void updateStartKey(Partition index, Group data,TableType tableType) {
        if (data == null) {
            return;
        }
        byte[] startKey = index.getStartKey();
        byte[] partitionKey = getKeyByGroup(data,tableType);
        if (startKey == null || Bytes.compareTo(partitionKey, startKey) < 0 ) {
            _logSyncLog.trace("update startKey {} -> {}",Bytes.toHex(startKey),Bytes.toHex(partitionKey));
            index.setEndKey(partitionKey);
        }
    }

    public static void updateIndexStatus(Partition indexNew) {
        if (indexNew.getStatus() == PartitionIndex.STATUS_NODATA) {
            indexNew.setStatus(PartitionIndex.STATUS_NEW);
        }
        else if (indexNew.getStatus() == PartitionIndex.STATUS_NEW) {
            indexNew.setStatus(PartitionIndex.STATUS_UPDATE);
        }
    }

    public static boolean checkPatition(long maxSizeParttion,
            long maxRowCountPartition,
            long datasize, 
            long rowCount, TableName tableInfo) {
        if ((maxSizeParttion > 0 && datasize >= maxSizeParttion)
                || (maxRowCountPartition > 0 && rowCount >= maxRowCountPartition)) {
            return true;
        }
        return false;
    }
    
    public static String addPartition(ConcurrentSkipListMap<byte[],Partition> mergePartitions,Partition index) throws Exception {
        if (index.getId() == -1) {
            throw new OrcaObjectStoreException("partition index id is null {}", index);
        }
        index.setCreateTimestamp(System.currentTimeMillis());
         
        mergePartitions.put(index.getStartKey(),index);
         
        return index.getDataFileName();
    }
    
    public static void updatePartition(ConcurrentSkipListMap<byte[],Partition> mergePartitions, Partition newIndex) throws Exception {
        mergePartitions.put(newIndex.getStartKey(), newIndex);
    }
    

    public static Partition getPartitionIndexByRowkey(
            ConcurrentSkipListMap<byte[],Partition> mergePartitions,
            byte[] searchRowKey, 
            long maxRowCountPartition, 
            long maxSizeParttion) {
        Map.Entry<byte[], Partition> entry = mergePartitions.floorEntry(searchRowKey);
        if(entry!=null) {
            Partition index = entry.getValue();
            if(Bytes.compareTo(searchRowKey, index.getEndKey())<=0 
                    || !isFull(maxRowCountPartition,
                            maxSizeParttion,
                            index.getRowCount(),
                            index.getDataSize() )) {
                return index;
            }
        }
        return null;
    }
    
    private static boolean isFull(long maxRowCountPartition, long maxSizeParttion, Long rowCount, Long dataSize) {
        if (maxRowCountPartition > 0 
                && maxSizeParttion > 0 
                && maxRowCountPartition <= rowCount 
                && maxSizeParttion <= dataSize ) {
            return true;
        }
        else if (maxRowCountPartition > 0 
                && maxRowCountPartition <= rowCount) {
            return true;
        }
        else if (maxSizeParttion > 0 
                && maxSizeParttion <= dataSize) {
            return true;
        }
        return false;
    }
    
    public static boolean isDeletedData(Group group) {
        int statusExists = group.getFieldRepetitionCount(Helper.SYS_COLUMN_STATUS);
        if (statusExists > 0) {
            byte[] status = group.getBinary(Helper.SYS_COLUMN_STATUS, 0).getBytes();
            if (Bytes.equals(Helper.SYS_COLUMN_STATUS_DELETE, status)) {
                return true;
            }
        }
        return false;
    }

    public static byte[] getKeyByGroup(Group group,TableType tableType) {
        String fieldName = null;
        if(tableType == TableType.INDEX) {
            fieldName = Helper.SYS_COLUMN_INDEXKEY_BYTES;
        }
        else if(tableType == TableType.DATA) {
            fieldName = Helper.SYS_COLUMN_PARQUETKEY_BYTES;
        }
        else {
            throw new OrcaObjectStoreException("table type error, {}",tableType);
        }
        byte[] key = null;
        int fieldCount = group.getFieldRepetitionCount(fieldName);
        if (fieldCount > 0) {
            key = group.getBinary(fieldName, 0).getBytes();
        }
        return key;
    }

    public static long getRowsCount(ConcurrentSkipListMap<byte[], Partition> mergePartitions) {
        long count = 0;
        if(mergePartitions !=null && mergePartitions.size()>0) {
            for(Partition partition : mergePartitions.values()) {
                count += partition.getRowCount();
            }
        }
        return count;
    }

    public static void synclog(TableName tableInfo,Group group,TableType tableType,String pos) {
        if (tableInfo.getTableId() < 0x100) {
            return;
        }
        String fun = "add";
        if(MergerUtils.isDeletedData(group)) {
            fun = "delete";
        }
        if(_logSyncLog.isTraceEnabled()){
            if(tableType == TableType.DATA) {
                _logSyncLog.trace("pos={} write table table={} fun={} rowKey={}",
                  pos,
                  tableInfo.getTableName(),
                  fun,
                  Bytes.toHex(group.getBinary(Helper.SYS_COLUMN_PARQUETKEY_BYTES, 0).getBytes())
                  );
            }else if(tableType == TableType.INDEX) {
                byte[] rowKey = null;
                if(!"delete".equalsIgnoreCase(fun)) {
                    rowKey = group.getBinary(Helper.SYS_COLUMN_PARQUETKEY_BYTES, 0).getBytes();
                }
                byte[] key = group.getBinary(Helper.SYS_COLUMN_INDEXKEY_BYTES, 0).getBytes();
                _logSyncLog.trace("pos={} write index table={} fun={} rowKey={} indexKey={}",
                        pos,
                        tableInfo.getTableName(),
                        fun,
                        rowKey==null ?"":Bytes.toHex(rowKey),
                        Bytes.toHex(key)
                        );
            }
        }
    }
    
    public static Group updateMinGroup(Group minData, Group group,TableType tableType) {
        
        if (minData == null) {
            minData = group;
        }
        else {
            byte[] rowKey = MergerUtils.getKeyByGroup(group,tableType);
            byte[] dataRowKey = MergerUtils.getKeyByGroup(minData,tableType);
            if (Bytes.compareTo(rowKey,dataRowKey) < 0) {// group is small
                minData = group;
            }
        }
        return minData;
    }
    
    public static Group updateMaxGroup(Group lastData, Group group,TableType tableType,int a) {
       
        if (lastData == null) {
            lastData = group;
        }
        else {
            byte[] rowKey = MergerUtils.getKeyByGroup(group,tableType);
            byte[] dataRowKey = MergerUtils.getKeyByGroup(lastData,tableType);
            if (Bytes.compareTo(rowKey,dataRowKey) > 0) {// group is big
                lastData = group;
            }
        }
        return lastData;
    }
    
    public static String showGroupContent(Group data) {
        StringBuffer sb = new StringBuffer();
        GroupType groupType = data.getType();
        List<Type> fields = groupType.getFields();
         

        for (Type field : fields) {
            String colName = field.getName();
            int valCount = data.getFieldRepetitionCount(colName);
            if(valCount <= 0) {
                continue;
            }
             
            sb.append(colName).append("=");
          
            try {
                PrimitiveType.PrimitiveTypeName typeStr = data.getType().getType(colName).asPrimitiveType()
                        .getPrimitiveTypeName();
                if (typeStr == PrimitiveType.PrimitiveTypeName.BINARY) {
                    Binary binaryVal = data.getBinary(colName, 0);
                    if (binaryVal != null) {
                        byte[] colValueBytes = binaryVal.getBytes();

                        OriginalType originalType = data.getType().getType(colName).getOriginalType();
                        if (originalType == OriginalType.DECIMAL) {
                            int precision = groupType.getType(colName).asPrimitiveType().getDecimalMetadata()
                                    .getPrecision();
                            int scale = groupType.getType(colName).asPrimitiveType().getDecimalMetadata().getScale();
                            BigDecimal decimalValue = Helper.binaryToDecimal(data.getBinary(colName, 0), precision,
                                    scale);
                            sb.append(decimalValue.stripTrailingZeros().toPlainString());
                        }
                        else {
                            sb.append(bytesToHexString(colValueBytes));
                        }
                    }
                }
                else if (typeStr == PrimitiveType.PrimitiveTypeName.INT96) {
                    Long colValue = data.getLong(colName, 0);
                    sb.append(colValue);
                }
                else if (typeStr == PrimitiveType.PrimitiveTypeName.INT64) {
                    Long colValue = data.getLong(colName, 0);
                    sb.append(colValue);
                }
                else if (typeStr == PrimitiveType.PrimitiveTypeName.INT32) {
                    Integer colValue = data.getInteger(colName, 0);
                    sb.append(colValue);
                }
                else if (typeStr == PrimitiveType.PrimitiveTypeName.BOOLEAN) {
                    Boolean colValue = data.getBoolean(colName, 0);
                    sb.append(colValue);
                }
                else if (typeStr == PrimitiveType.PrimitiveTypeName.DOUBLE) {
                    Double colValue = data.getDouble(colName, 0);
                    sb.append(colValue);
                }
                else if (typeStr == PrimitiveType.PrimitiveTypeName.FLOAT) {
                    Float colValue = data.getFloat(colName, 0);
                    sb.append(colValue);
                }
                else if (typeStr == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
                    GroupType type = data.getType();
                    OriginalType originalType = type.getType(colName).getOriginalType();
                    if (originalType != null && originalType.name().equals("DECIMAL")) {

                        int precision = type.getType(colName).asPrimitiveType().getDecimalMetadata().getPrecision();
                        int scale = type.getType(colName).asPrimitiveType().getDecimalMetadata().getScale();
                        BigDecimal decimalValue = Helper.binaryToDecimal(data.getBinary(colName, 0), precision, scale);
                        sb.append(decimalValue.stripTrailingZeros().toPlainString());

                    }
                    else {
                        Binary binaryVal = data.getBinary(colName, 0);
                        if (binaryVal != null) {
                            byte[] colValueBytes = binaryVal.getBytes();
                            sb.append( bytesToHexString(colValueBytes));
                        }
                    }
                }
                else {
                    throw new NotImplementedException();
                }
            }
            catch (RuntimeException e) {
                // _log.error(e.getMessage(), e);
            }
            sb.append("\t");
        }
        return sb.toString();
    }
    
    private static String bytesToHexString(byte[] src) {
        StringBuilder stringBuilder = new StringBuilder("");
        if (src == null || src.length <= 0) {
            return null;
        }
        for (int i = 0; i < src.length; i++) {
            int v = src[i] & 0xFF;
            String hv = Integer.toHexString(v);
            if (hv.length() < 2) {
                stringBuilder.append(0);
            }
            stringBuilder.append(hv);
        }
        return stringBuilder.toString();
    }
}
