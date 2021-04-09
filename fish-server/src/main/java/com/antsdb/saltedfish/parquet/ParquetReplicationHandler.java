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

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.nosql.DeleteRowEntry2;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.Replicable;
import com.antsdb.saltedfish.nosql.ReplicationHandler2;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.SlowRow;
import com.antsdb.saltedfish.nosql.StorageTable;
import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.nosql.SysNamespaceRow;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.obs.action.ActionDeleteFolder;
import com.antsdb.saltedfish.obs.action.UploadSet;
import com.antsdb.saltedfish.obs.cache.ObsCache;
import com.antsdb.saltedfish.util.UberUtil;

/**
 *
 * @author Frank Li<lizc@tg-hd.com>
 */
public class ParquetReplicationHandler implements ReplicationHandler2, Replicable {
    static Logger _log = UberUtil.getThisLogger();
    static Logger _logSyncLog = LoggerFactory.getLogger(ParquetReplicationHandler.class.getName() + ".sync-log");
    
    Humpback humpback;
    ObsService service;
    long trxCount = 0;
    
    private ReplicationAssistant assistant;

    private long replicateLogPointer ;
    private MessageTypeSchemaUtils schemaUtils;
    private ObsCache obsCache;
    
    private UploadSet uploadSet;
    private boolean isMutable;
    
    public ParquetReplicationHandler(Humpback humpback, 
            ObsService storageService, 
            MessageTypeSchemaUtils schemaUtils,
            ObsCache obsCache,
            boolean isMutable
            ) {
        this.humpback = humpback;
        this.service = storageService;
        this.assistant = null;
        this.schemaUtils = schemaUtils;
        this.obsCache = obsCache;
        this.isMutable = isMutable;
        this.replicateLogPointer = this.service.getCommittedLogPointer();
    }

    /**
     * 当前同步的位置
     */
    @Override
    public long getReplicateLogPointer() {
        return this.replicateLogPointer;
    }

    @Override
    public ReplicationHandler2 getReplayHandler() {
        return this;
    }
    /**
     * 
     */
    
    @Override
    public long getCommittedLogPointer() {
        long cursp = this.service.getCommittedLogPointer();
        return cursp;
    }

    @Override
    public void flush(long lpRows, long lpIndexes) throws Exception {
        this.replicateLogPointer = lpRows;
        this.service.startCommit(lpRows, this.uploadSet);
        this.assistant = null;
    }

    @Override
    public void putRow(int tableId, long pRow, long version, long pEntry, long lpLogRow) throws Exception {
        if (!this.isMutable) {
            throw new OrcaObjectStoreException("obs storage is in read-only mode");
        }
        
        if (tableId < 0) {
            // skip temporary table
            return;
        }
        long committedSp = this.getReplicateLogPointer();
        if (tableId > 0x100 && lpLogRow < committedSp) {
            throw new OrcaObjectStoreException("tableId={} putRow new sp={} is less cur sp ={} is error",
                    tableId,
                    lpLogRow,
                    committedSp
                    ) ;
        }
        
        initTableInfo(tableId);
        
        if(isTAbleDelete()) {
            _log.warn("put row table={} is delete or not exists",tableId);
            return;
        }
        
        Group group = assistant.getFactory().newGroup();
        Row row = Row.fromMemoryPointer(pRow, version);
        //checksyncSortTest(tableId,row);
        checkDropTable(tableId,row);
        
        group.append(Helper.SYS_COLUMN_STATUS, Binary.fromConstantByteArray(Helper.SYS_COLUMN_STATUS_INSERT));

        row2Group(row,group,version);
        synclog(tableId,group,"add",TableType.DATA);       
        group2Merge(tableId,group,TableType.DATA);
    }
    
    private boolean isTAbleDelete() {
        if(this.assistant.getTableInfo() == null || assistant.getTable().isDelete()) {
            _log.trace("table={} is deleted",this.assistant.getTableId());
            return true;
        }
        return false;
    }

    private void synclog(int tableId,Group group,String fun,TableType type) {
        if (tableId < 0x100) {
            return;
        }
        if(_logSyncLog.isTraceEnabled()){
            if(type == TableType.DATA) {
                _logSyncLog.trace("sync table table={} fun={} rowKey={}",
                  this.assistant.getTableId(),
                  fun,
                  Bytes.toHex(group.getBinary(Helper.SYS_COLUMN_PARQUETKEY_BYTES, 0).getBytes())
                  );
            }else if(type == TableType.INDEX) {
                byte[] rowKey = null;
                if(!"delete".equalsIgnoreCase(fun)) {
                    rowKey = group.getBinary(Helper.SYS_COLUMN_PARQUETKEY_BYTES, 0).getBytes();
                }
                byte[] key = group.getBinary(Helper.SYS_COLUMN_INDEXKEY_BYTES, 0).getBytes();
                _logSyncLog.trace("sync index table={} fun={} rowKey={} indexKey={}",
                        this.assistant.getTableId(),
                        fun,
                        rowKey==null ?"":Bytes.toHex(rowKey),
                        Bytes.toHex(key)
                        );
            }
        }
              
    }
    
//    private void checksyncSortTest(int tableId,Row row) {
//        
//        if(tmpTableId == -1 || tmpTableId != tableId) {
//            tmpTableId = tableId;
//            
//            this.syncKey = new byte[] {0};
//            syncKeyAddress = 0;
//            
//        }else {
//            long keyAddress = row.getKeyAddress();
//            byte[] rowkey = Helper.antsKeyToHdfs(keyAddress);
//            
//            if(tmpTableId ==  tableId && Bytes.compareTo(rowkey, this.syncKey) < 0) {
//                _log.debug("cur row key is small tableId:{} old:{},new:{},tostring old-keyAddress :{},new:{},old-keyAddress :{},new:{}",
//                        tableId,
//                        Bytes.toHex(syncKey),
//                        Bytes.toHex(rowkey),
//                        KeyBytes.toString(syncKeyAddress),
//                        KeyBytes.toString(keyAddress),
//                        syncKeyAddress,
//                        keyAddress
//                        );
//            }
//            this.syncKeyAddress = keyAddress;
//            this.syncKey = rowkey;
//        }
//       
//    }
    
    private void checkDropTable(int tableId,Row row) throws IOException, Exception {
        if(tableId == 0) {
            SlowRow slowrow = SlowRow.from(row);
            SysMetaRow tablemeta = new SysMetaRow(slowrow);
            if(tablemeta.isDeleted()) {// drop table 
                TableName delTableInfo = TableName.valueOf(tablemeta);
                _log.debug("drop table:{}", delTableInfo.getTablePath());
                StorageTable storageTable = this.service.getTable(delTableInfo.getTableId());
                if(storageTable == null) {
                    return ;
                }
                PartitionIndex partitionIndex = storageTable.getPartitionIndex();
                if(partitionIndex == null) {
                    try {
                        partitionIndex = new PartitionIndex(
                                this.service.getLocalDataHome(),
                                this.obsCache,
                                delTableInfo,
                                service.getInitPartitiFile(delTableInfo.getTableId())
                                );
                        storageTable.setPartitionIndex(partitionIndex);
                    }catch(Exception e){
                        throw new OrcaObjectStoreException(e);
                    }
                }
                String partitionIndexFileName = partitionIndex.getIndexFileVersionPath();
                
                ActionDeleteFolder action = new ActionDeleteFolder(delTableInfo.getTablePath(),partitionIndexFileName);
                this.uploadSet.getDropActions().add(action);
                
                _log.debug("drop table:{},partitionIndex:{}", 
                        delTableInfo.getTablePath(),
                        partitionIndexFileName);
                
                this.service.syncDropEntity(delTableInfo );
            }
        }
        
    }

    private void row2Group(Row row,Group group,long version) {
        Column[] columnObjs = assistant.getMapping().getQualifiers();
        if (columnObjs != null && columnObjs.length >= 0) {
            row2GroupByMapping(
                    assistant.getTableId(), 
                    assistant.getMapping(), 
                    row, 
                    group,
                    assistant.getSchema(),
                    version);
        }
        else {
            row2GroupBySchema(
                    assistant.getTableId(), 
                    row, 
                    group, 
                    assistant.getSchema(),
                    version);
        }
    }

    private void initTableInfo(int tableId) throws Exception {
        if(assistant == null) {
            uploadSet = new UploadSet();
        }
        if(assistant == null) {
            assistant = new ReplicationAssistant();
            assistant.setTableId(tableId); 
            _log.trace("event start table:==>>{}",tableId);
            TableName tableInfo = service.getTableName(tableId);
            assistant.setTableInfo(tableInfo);
            
            Mapping mapping = service.getMapping(tableId);
            assistant.setMapping(mapping);
            StorageTable table = this.service.getTable(tableId);
            if(table!=null) {
                initStorageTable(assistant,tableId, table);
            }
            else {
                _log.debug("(2)StorageTable not exist,tableId={} ", tableId);
                throw new OrcaObjectStoreException("StorageTable ìs null,talbe={}",tableId);
            }
            assistant.setTable(table);
            GroupFactory factory = new SimpleGroupFactory(assistant.getSchema());
            assistant.setFactory(factory);
        }
        else if(assistant.getTableId() != tableId) {
            _log.trace("table change,{}==>>{}",assistant.getTableId(),tableId);
            this.service.changeTableMerger(this.uploadSet);
            
            assistant = new ReplicationAssistant();
            assistant.setTableId(tableId) ;
            TableName tableInfo = service.getTableName(tableId);
            assistant.setTableInfo(tableInfo);
            Mapping mapping = service.getMapping(tableId);
            assistant.setMapping(mapping);
            StorageTable table = this.service.getTable(tableId);
            if(table!=null) {
                initStorageTable(assistant,tableId, table);
            }
            else {
                _log.debug("(4)StorageTable not exist,tableId={}",tableId);
                throw new OrcaObjectStoreException("StorageTable ìs null,talbe={} assistant={}",
                        tableId,
                        assistant.getTableId());
            }
            assistant.setTable(table);
            GroupFactory factory = new SimpleGroupFactory(assistant.getSchema());
            assistant.setFactory(factory);
        }
    }

    private void initStorageTable(ReplicationAssistant assistant,int tableId,StorageTable table) {
        MessageType schema = table.getSchema();
        TableType tableType = table.getRowMeta().getType();
        if(schema == null) {
            TableName tableInfo = service.getTableName(tableId);
            _log.debug("(1)schema is empty,tableId={} reload ...", tableId);
            schema = this.schemaUtils.getSchema(tableInfo, assistant.getMapping(),tableType);
            table.setSchema(schema);
            assistant.setSchema(schema);
        }
        else {
            boolean reload = checkSchema(tableId,assistant,table,schema.getFields());
            if(reload) {
                _log.debug("(5)checkSchema schem is old flag,tableId={} flag={},reload ...",
                        tableId,
                        reload);
                TableName tableInfo = service.getTableName(tableId);
                schema = this.schemaUtils.getSchema(tableInfo, assistant.getMapping(),tableType);
                table.setSchema(schema);
                assistant.setSchema(schema);
            }
            else {
                assistant.setSchema(schema);
            }
        }
        if(table.getPartitionIndex() == null ) {
            try {
                TableName tableInfo = service.getTableName(tableId);
                PartitionIndex partitionIndex = new PartitionIndex(
                        this.service.getLocalDataHome(),
                        this.obsCache,
                        tableInfo,
                        this.service.getInitPartitiFile(tableId)
                        );
                table.setPartitionIndex(partitionIndex);
            }catch(Exception e){
                throw new OrcaObjectStoreException(e);
            }
        }
    }

    private boolean checkSchema(int tableId,ReplicationAssistant assistant,StorageTable table, List<Type> fields) {
        boolean reLoadSchema = false;
        if (fields != null && fields.size() > 0 ) {
            Column[]  columns = assistant.getMapping().getQualifiers();
            
            if(columns!=null) {
                 
                for(Column column : columns) {
                    if(column == null) {
                        continue;
                    }
                    boolean exists = isExists(column,fields);
                    if(exists) {
                        continue;
                    }
                    else {
                        _log.debug("column:{} ont found reload Schema,tableId={}",
                                column.getName(),
                                tableId
                                );
                        reLoadSchema = true;
                        break;
                    }
                }
            }
        }
        else {
            reLoadSchema = true; 
        }

        return reLoadSchema;
        
    }

    private boolean isExists(Column column,List<Type> fields) {
        for (Type field : fields) {
            String columnName = field.getName();
            if(column.getName().equals(columnName)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * tableId
     * pIndexKey 
     * 
     */
    @Override
    public void putIndex(int tableId, long pIndexKey, long pIndex, long version, long pEntry, long lpEntry)
            throws Exception {
        if (!this.isMutable) {
            throw new OrcaObjectStoreException("obs storage is in read-only mode");
        }
        if (tableId < 0) {
            return;
        }
        long committedSp = this.getReplicateLogPointer();
        if (tableId > 0x100 && lpEntry < committedSp) {
            throw new OrcaObjectStoreException("tableId:\tputIndex new sp={} is less cur sp ={} is error",
                    tableId,
                    lpEntry,
                    committedSp
                    ) ;
        }
        
        initTableInfo(tableId);
        if(isTAbleDelete()) {
            _log.warn("put index table={} is delete or not exists",tableId);
            return;
        }
        Group group = assistant.getFactory().newGroup();

        byte miscByte = new KeyBytes(pIndex).getSuffixByte();
        int size = KeyBytes.getRawSize(pIndexKey) + KeyBytes.getRawSize(pIndex) + 1;
        byte[] rowkey  = Helper.antsKeyToHdfs(pIndex);
        byte[] indexKey= Helper.antsKeyToHdfs(pIndexKey);
        
        
        byte[] misc = new byte[1];
        misc[0] = miscByte;
        
        group.append(Helper.SYS_COLUMN_PARQUETKEY_BYTES, Binary.fromReusedByteArray(rowkey));
        group.add(Helper.SYS_COLUMN_INDEXKEY_BYTES, Binary.fromReusedByteArray(indexKey));
        group.append(Helper.SYS_COLUMN_MISC_BYTES, Binary.fromReusedByteArray(misc));
        group.append(Helper.SYS_COLUMN_SIZE_BYTES, size);
        group.append(Helper.SYS_COLUMN_VERSION_BYTES, version); 

        synclog(tableId,group,"add",TableType.INDEX);
        
        group2Merge(tableId,group,TableType.INDEX);
    }

    @Override
    public void deleteRow(int tableId, long pKey, long version, long pEntry, long lpLogDeleteRow) throws Exception {
        if (!this.isMutable) {
            throw new OrcaObjectStoreException("obs storage is in read-only mode");
        }
        if (tableId < 0) {
            return;
        }
        
        initTableInfo(tableId);
        if(isTAbleDelete()) {
            _log.warn("delete row table={} is delete or not exists",tableId);
            return;
        }
        
        Group group = assistant.getFactory().newGroup();
        byte[] key = Helper.antsKeyToHdfs(pKey);
        group.append(Helper.SYS_COLUMN_PARQUETKEY_BYTES, Binary.fromConstantByteArray(key));
        group.append(Helper.SYS_COLUMN_STATUS,
                Binary.fromConstantByteArray(Helper.SYS_COLUMN_STATUS_DELETE));
        synclog(tableId,group,"delete",TableType.DATA);
        group2Merge(tableId,group,TableType.DATA);
        
        // drop database 
        if(tableId == 1) {
            DeleteRowEntry2 entry = new DeleteRowEntry2(lpLogDeleteRow, pEntry);
            if(entry!=null) {
                SlowRow slowrow = SlowRow.fromRowPointer(entry.getRowPointer(), version);
                if(slowrow!=null) {
                    SysNamespaceRow spaceRow = new SysNamespaceRow(slowrow);
                    String nameSpace =  spaceRow.getNamespace();
                    if(nameSpace != null && nameSpace.length() >0) {
                        ActionDeleteFolder action = new ActionDeleteFolder(nameSpace);
                        this.uploadSet.getDropActions().add(action);
                        _log.debug("drop database:{}", nameSpace);
                    }
                }
            }
        }
    }

    @Override
    public void deleteIndex(int tableId, long pKey, long version, long pEntry, long lpDelete) throws Exception {
        if (!this.isMutable) {
            throw new OrcaObjectStoreException("obs storage is in read-only mode");
        }
        if (tableId < 0) {
            return;
        }

        initTableInfo(tableId);
        if(isTAbleDelete()) {
            _log.warn("delete index table={} is delete or not exists",tableId);
            return;
        }
        
        byte[] key = Helper.antsKeyToHdfs(pKey);
        Group group = assistant.getFactory().newGroup();
        //group.append(Helper.SYS_COLUMN_PARQUETKEY_BYTES, Binary.fromConstantByteArray(key));
        group.append(Helper.SYS_COLUMN_INDEXKEY_BYTES, Binary.fromConstantByteArray(key));
        group.append(Helper.SYS_COLUMN_STATUS, Binary.fromConstantByteArray(Helper.SYS_COLUMN_STATUS_DELETE));

        synclog(tableId,group,"delete",TableType.INDEX);        
        group2Merge(tableId,group,TableType.INDEX);
    }

    private void group2Merge(int tableId,Group group,TableType tableType) throws Exception {
        if(tableId!=assistant.getTableId()) {
            throw new OrcaObjectStoreException("table error tableId={} tableType={} tablieInfo={}",tableId,tableType,assistant.getTableId()); 
        }
        try {
            service.startMerge(assistant.getTableInfo(), group, assistant.getSchema(),tableType);
        }catch(Exception e){
            _log.warn("tableId={} tableType={} assistant={} exception={}",
                    tableId,
                    tableType,
                    assistant.getTableId(),
                    e.getMessage());
            throw e; 
        }
    }
    
    private void row2GroupByMapping(int tableId,
            Mapping mapping, 
            Row row, 
            Group group,
            MessageType schema,
            long version) {

        byte[] rowkey = Helper.antsKeyToHdfs(row.getKeyAddress());
        int sizes = row.getLength();
       
        group.append(Helper.SYS_COLUMN_PARQUETKEY_BYTES, Binary.fromReusedByteArray(rowkey));
        group.append(Helper.SYS_COLUMN_HASH_BYTES, row.getHash());
        group.append(Helper.SYS_COLUMN_SIZE_BYTES, sizes);
        group.append(Helper.SYS_COLUMN_VERSION_BYTES,version); 
        // populate fields
        int maxColumnId = row.getMaxColumnId();
        byte[] types = new byte[maxColumnId + 1];
        for (int i = 0; i <= maxColumnId; i++) {
            long pValue = row.getFieldAddress(i);
            types[i] = Helper.getType(pValue);
            Object val = FishObject.get(null, pValue);// Helper.toBytes(pValue);
            Column column = mapping.getColumn(i);
            if (column != null) {
                String columnName = column.getName();
                int antsdbType = column.getType();
                PrimitiveTypeName typeName = MessageTypeSchemaUtils.antsdbType2Parquet(antsdbType);

                if (val == null) {
                    if (schema.getType(columnName).getRepetition() != Type.Repetition.REQUIRED) {
                        continue;
                    }
                    else {
                        throw new IllegalArgumentException("cloumn (" + column + ") is REQUIRED , value is required");
                    }
                }
                if (columnName.startsWith("*")) {
                    if (typeName.equals(PrimitiveTypeName.BINARY)) {
                        group.append(columnName, ParquetUtils.toBinary(val));
                    }
                    else if (typeName == PrimitiveType.PrimitiveTypeName.INT64) {
                        group.append(columnName, ParquetUtils.toInt64(val));
                    }
                    else if (typeName == PrimitiveType.PrimitiveTypeName.INT32) {
                        group.append(columnName, ParquetUtils.toInt32(val));
                    }
                    else if (typeName == PrimitiveType.PrimitiveTypeName.BOOLEAN) {
                        group.append(columnName, ParquetUtils.toBoolean(val));
                    }
                    else if (typeName == PrimitiveType.PrimitiveTypeName.DOUBLE) {
                        group.append(columnName, ParquetUtils.toDouble(val));
                    }
                    else if (typeName == PrimitiveType.PrimitiveTypeName.FLOAT) {
                        group.append(columnName, ParquetUtils.toFloat(val));
                    }
                    else {
                        throw new NotImplementedException();
                    }
                }
                else {
                    switch (antsdbType) {
                        case Value.TYPE_NUMBER:
                        case Value.FORMAT_INT4:
                        case Value.FORMAT_INT8:
                            group.append(columnName, ParquetUtils.toInt64(val));
                            break;
                        case Value.FORMAT_BIGINT:
                            group.append(columnName, ParquetUtils.toBinary(val));
                            break;
                        case Value.FORMAT_FAST_DECIMAL:
                        case Value.FORMAT_DECIMAL:
                            group.append(columnName, ParquetUtils.toBinary(val));
                            break;
                        case Value.TYPE_STRING:
                        case Value.FORMAT_UTF8:
                        case Value.FORMAT_UNICODE16:
                            group.append(columnName, ParquetUtils.toBinary(val));
                            break;
                        case Value.TYPE_BOOL:
                            // case Value.FORMAT_BOOL:
                            group.append(columnName, ParquetUtils.toBoolean(val));
                            break;
                        case Value.TYPE_DATE:
                        case Value.TYPE_TIME:
                        case Value.TYPE_TIMESTAMP:
                            // case Value. FORMAT_TIME:
                            // case Value. FORMAT_DATE:
                            // case Value. FORMAT_TIMESTAMP:
                            group.append(columnName, ParquetUtils.toInt64ByDatetime(val));
                            break;
                        case Value.FORMAT_FLOAT4:
                            group.append(columnName, ParquetUtils.toFloat(val));
                            break;
                        case Value.FORMAT_FLOAT8:
                            group.append(columnName, ParquetUtils.toDouble(val));
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
                            group.append(columnName, ParquetUtils.toBinary(val));
                            break;
                        default:
                            group.append(columnName, ParquetUtils.toBinary(val));
                    }
                }
            }
            else if (pValue != 0) {
                String msg = String.format("table=%d mapping.tableId=%s ns=%s tableName=%s address=%s pValue=%s", 
                        tableId,
                        mapping.tableId,
                        mapping.ns ,
                        mapping.tableName ,
                        KeyBytes.toString(row.getKeyAddress()), pValue);
                throw new IllegalArgumentException(msg);
            }
        }
        group.append(Helper.SYS_COLUMN_DATATYPE_BYTES, Binary.fromReusedByteArray(types));
    }

    private void row2GroupBySchema(int tableId,Row row, Group group, MessageType schema,long version) {
        byte[] rowkey = Helper.antsKeyToHdfs(row.getKeyAddress());
        int sizes = row.getLength();
        group.append(Helper.SYS_COLUMN_PARQUETKEY_BYTES, Binary.fromReusedByteArray(rowkey));
        group.append(Helper.SYS_COLUMN_HASH_BYTES, row.getHash());
        group.append(Helper.SYS_COLUMN_SIZE_BYTES, sizes);
        group.append(Helper.SYS_COLUMN_VERSION_BYTES, version); 
        // populate fields
        int maxColumnId = row.getMaxColumnId();
        byte[] types = new byte[maxColumnId + 1];
        for (int i = 0; i <= maxColumnId; i++) {
            long pValue = row.getFieldAddress(i);
            types[i] = Helper.getType(pValue);
            Object val = FishObject.get(null, pValue);// Helper.toBytes(pValue);
            String columnName = schema.getFieldName(i);
            if (val == null) {
                continue;
            }
            if (columnName != null) {
                PrimitiveTypeName typeName = PrimitiveTypeName.BINARY;

                if (typeName.equals(PrimitiveTypeName.BINARY)) {
                    if(columnName.startsWith("*")) {
                        group.append(columnName, ParquetUtils.toBinary(val));
                    }
                    else {
                        group.append(columnName, ParquetUtils.toBinary(val));
                    }
                }
                else {
                    throw new NotImplementedException();
                }
            }
            else if (pValue != 0) {
                String msg = String.format("tablieId=%d address=%s pValue=%s", 
                        tableId,
                        KeyBytes.toString(row.getKeyAddress()),
                        pValue);
                throw new IllegalArgumentException(msg);
            }
        }
        group.append(Helper.SYS_COLUMN_DATATYPE_BYTES, Binary.fromReusedByteArray(types));
    }
}
