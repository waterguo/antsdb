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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.nosql.ConfigService;
import com.antsdb.saltedfish.nosql.HColumnRow;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.Replicable;
import com.antsdb.saltedfish.nosql.StorageTable;
import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.obs.ObsProvider;
import com.antsdb.saltedfish.obs.action.ActionUploadSyncParam;
import com.antsdb.saltedfish.obs.action.UploadSet;
import com.antsdb.saltedfish.obs.cache.ObsCache;
import com.antsdb.saltedfish.obs.upload.ExecutorBoatPool;
import com.antsdb.saltedfish.obs.upload.ExecutorCatchPool;
import com.antsdb.saltedfish.obs.upload.FishBoat;
import com.antsdb.saltedfish.parquet.bean.Partition;
import com.antsdb.saltedfish.parquet.bean.SyncParam;
import com.antsdb.saltedfish.parquet.merge.TableMerger;
import com.antsdb.saltedfish.server.SaltedFish;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.util.LongLong;
import com.antsdb.saltedfish.util.SizeConstants;
import com.antsdb.saltedfish.util.UberTime;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author Frank Li<lizc@tg-hd.com>
 */
public abstract class BaseStorageService implements ObsService {
    final static Logger _log = UberUtil.getThisLogger();
    
    protected boolean isMutable;
    protected Humpback humpback = null; // humpback for handler to use
    protected MetadataService metaService = null; // MetadataService to get Table Meta from ANTSDB
    protected CheckPoint checkPoint;
    protected int maxColumnPerPut = 2500; // maximum column included in one put(rows)
    protected CompressionCodecName compressionType = CompressionCodecName.UNCOMPRESSED;
    protected String sysns;
    protected TableName tnCheckpoint;
    protected volatile boolean isClosed;
    protected File home;
    protected File localDataDirectory;
    protected int partitionMaxDatasize;
    protected int partitionMaxRowCount;
    protected ObsProvider provider;
    protected ConcurrentMap<Integer, StorageTable> tableById = new ConcurrentHashMap<>();
    protected ParquetReplicationHandler replicationHandler;
    protected MessageTypeSchemaUtils schemaUtils;
    protected ExecutorBoatPool  boatThreads;
    protected ExecutorCatchPool catchThreads;
    protected ObsCache obsCache;
    
    private TableMerger tableMerger;
    protected long replicateLogPointer ;
    
    private ConcurrentMap<Integer, String> tablePartitionIndexs = new ConcurrentHashMap<>();
    
    private boolean md5Flag;
    
    protected void setup() throws Exception {
        if (!this.isMutable) {
            return;
        }
        if (!Helper.existsNamespace(this.provider,this.getRemoteDataHome(), this.sysns)) {
            _log.info("namespace {} is not found in remote store, creating ...", this.sysns);
            createNamespace(this.sysns);
        }
        String objectKey = this.getRemoteDataHome() + this.tnCheckpoint.getTableName()
                            + ParquetUtils.DATA_JSON_EXT_NAME;
        if (! this.provider.doesObjectExist(objectKey)) {
            _log.info("checkpoint table {} is not found in remote store, creating ...", this.tnCheckpoint);
            CheckPoint checkPoint = new CheckPoint(this.tnCheckpoint,isMutable);
            checkPoint.setServerId(this.humpback.getServerId());
             
            SyncParam syncParam = checkPoint.writeSyncParam(this.getLocalDataHome(),
                    this.getRemoteDataHome(),
                    this.provider,
                    this.obsCache);
            _log.info("antsdb start current commit sp :{}",syncParam.getCurrentSp());
        }
    }

    @Override
    public void createNamespace(String namespace) throws OrcaObjectStoreException {
        if (!this.isMutable) {
            throw new OrcaObjectStoreException("obs storage is in read-only mode");
        }
        Helper.createNamespace(this.provider,this.getRemoteDataHome(), namespace);
        this.createDataAnalyticsDatabaseSchema(namespace);
    }

    @Override
    public void deleteNamespace(String namespace) throws OrcaObjectStoreException {
        if (!this.isMutable) {
            throw new OrcaObjectStoreException("obs storage is in read-only mode");
        }
        dropDataAnalyticsDatabaseSchema(namespace);
    }

    @Override
    public boolean exist(int tableId) {
        TableName tn = getTableName(tableId);
        return Helper.existsTable(
                this.provider,
                this.getRemoteDataHome(),
                tn);
    }

    @Override
    public void gc(long timestamp) {
        // nothing to gc
        throw new NotImplementedException();
    }

    @Override
    public boolean isTransactionRecoveryRequired() {
        return true;
    }

    @Override
    public void setEndSpacePointer(long sp) {
        throw new NotImplementedException();
    }

    @Override
    public void checkpoint() throws Exception {
        throw new NotImplementedException();
    }

    @Override
    public LongLong getLogSpan() {
        LongLong result = new LongLong(0, this.checkPoint.getCurrentSp());
        return result;
    }

    @Override
    public void setMetaService(MetadataService metaService) {
        this.metaService = metaService;
    }

    @Override
    public TableName getTableName(int tableId) {
        SysMetaRow metarow = this.humpback.getTableInfo(tableId);
        if (metarow == null) {
            throw new OrcaObjectStoreException("metadata of table {} is not found", tableId);
        }
        if (metarow.isDeleted()) {
            return null;
        }
        String ns = metarow.getNamespace();
        ns = (ns.equals(Orca.SYSNS)) ? this.sysns : ns;
        return TableName.valueOf(ns, metarow.getTableName(), tableId);
    }

    @Override
    public Mapping getMapping(int tableId) {
        SysMetaRow tableInfo = this.humpback.getTableInfo(tableId);// search x0
        if (tableInfo == null) {
            throw new OrcaObjectStoreException("humpback metadata for table {} is not found", tableId);
        }
        if (tableInfo.isDeleted()) {
            return null;
        }
        List<HColumnRow> columns = this.humpback.getColumns(tableId);// search x3
        if ((tableId >= 0x100) && (columns == null /* || columns.size() == 0 */)) {
            throw new OrcaObjectStoreException("orca metadata for table {} is not found", tableId);
        }
        Mapping mapping = new Mapping(this.sysns, tableInfo, columns);
        return mapping;
    }

    @Override
    public long getCommittedLogPointer() {
        if(this.checkPoint != null) {
            return this.checkPoint.getCurrentSp();
        }
        else {
            return -1;
        }
    }
    
    @Override
    public long getReplicateLogPointer() {
        return replicateLogPointer;
    }
    
    @Override
    public CheckPoint getCheckPoint() {
        return checkPoint;
    }

    @Override
    public void updateLogPointer(long currentSP) throws Exception {
        long hdfsLp = this.getReplicateLogPointer();
        if (currentSP > hdfsLp) {
            SyncParam syncParam = this.checkPoint.updateSyncParam(currentSP);
            if(syncParam!=null) {
                UploadSet uploadSet = new UploadSet();
                ActionUploadSyncParam uploadSyncParam = getUploadSyncParam(true,syncParam);
                uploadSet.setUploadSyncParam(uploadSyncParam);
                upload(uploadSet,true);
            }
        }
    }

    @Override
    public Map<String, Object> get_(String ns, String tn, int tableId, byte[] key) throws Exception {
        ns = (ns.equals(Orca.SYSNS)) ? this.sysns : ns;
        TableName tableName = TableName.valueOf(ns, tn, tableId);
        
        StorageTable storageTable = this.getTable(tableId);
        if(storageTable == null) {
            return null;
        }
        PartitionIndex partitionIndex = storageTable.getPartitionIndex();
        if(partitionIndex == null) {
            partitionIndex = new PartitionIndex(
                    getLocalDataHome(),
                    this.obsCache,
                    tableName,
                    getInitPartitiFile(tableName.getTableId())
                    );
            storageTable.setPartitionIndex(partitionIndex);
        }
        Partition partition =  partitionIndex.getPartitionIndexByRowkey(key);
        
        Result r = Helper.get(
                partition,
                this.getObsCache(),
                tableName, 
                key);
        if (r.isEmpty()) {
            return null;
        }
        Map<String, Object> row = new HashMap<>();
        row = r.toMap();
        return row;
    }

    @Override
    public boolean existsTable(String namespace, String tableName, int tableId) {
        return Helper.existsTable(
                this.provider,
                this.getRemoteDataHome(),
                namespace, 
                tableName, 
                tableId);
    }

    @Override
    public TableMeta getTableMeta(int tableId) {
        if (this.metaService == null) {
            return null;
        }
        return this.metaService.getTable(Transaction.getSeeEverythingTrx(), tableId);
    }

    @Override
    public String getSystemNamespace() {
        return this.sysns;
    }

    @Override
    public File getLocalDataHome() {
        return localDataDirectory;
    }

    @Override
    public String getRemoteDataHome() {
        return "";
    }

    public long get(Heap heap, int tableId, long trxid, long trxts, long pKey) throws Exception {
        TableName tableName = getTableName(tableId);
        if (tableName == null) {
            throw new OrcaObjectStoreException("table id {} is invalid", tableId);
        }
        try {
            byte[] key = Helper.antsKeyToHdfs(pKey);
            StorageTable storageTable = this.getTable(tableId);
            if(storageTable == null) {
                throw new OrcaObjectStoreException("StorageTable is null,tableId={} pKey={}",tableId,pKey)  ;
            }
            PartitionIndex partitionIndex = storageTable.getPartitionIndex();
            if(partitionIndex == null) {
                partitionIndex = new PartitionIndex(
                        getLocalDataHome(),
                        this.obsCache,
                        tableName,
                        getInitPartitiFile(tableName.getTableId())
                        );
                storageTable.setPartitionIndex(partitionIndex);
            }
            
            Partition partition =  partitionIndex.getPartitionIndexByRowkey(key);
            
            Result r = Helper.get(
                    partition,
                    this.getObsCache(),
                    tableName, 
                    key);
            TableMeta table = getTableMeta(tableId);
            return Helper.toRow(heap, r.getGroup(), table, tableId);
        }
        catch (IOException x) {
            throw new OrcaObjectStoreException(x);
        }
    }

    /**
     * called when antsdb trying to create a new column on a table
     * 
     * @param tableId
     * @param columnId
     * @param name not null
     * @param type see class Value
     */
    @Override
    public void createColumn(int tableId, int columnId, String fieldName, int type) {
        StorageTable table = tableById.get(tableId);
        if (table == null) {
            return;
        }
        TableName tableInfo = TableName.valueOf(table.getNamespace(), table.getTableName(), tableId);
        _log.debug(tableInfo.getTableName() + " create column id:{},name:{},type:{}", columnId, fieldName, type);

        MessageType newSchema = null;
        Mapping mapping = getMapping(tableId);

        if (mapping.getQualifiers() == null || mapping.getQualifiers().length == 0) {
            newSchema = MessageTypeSchemaUtils.genNewSchema(tableInfo, fieldName, type);
        }
        else {
            TableType tableType = table.getRowMeta().getType();
            MessageType schema = schemaUtils.getSchema(tableInfo, mapping,tableType);
            boolean merge = true;
            if (schema != null) {
                List<Type> fields = schema.getFields();
                if (fields != null && fields.size() > 0) {
                    for (Type field : fields) {
                        String columnName = field.getName();
                        if (fieldName.equals(columnName)) {
                            merge = false;
                        }
                    }
                }
            }
            if (merge) {
                newSchema = MessageTypeSchemaUtils.addFiledMessageType(schema, fieldName, type);
            }
        }
        if (newSchema != null) {
            table.setSchema(newSchema);
        }
    }

    /**
     * called when antsdb trying to delete a column on a table
     * @param tableId
     * @param columnId
     * @param columnName not null
     */
    @Override
    public void deleteColumn(int tableId, int columnId, String columnName) {
        StorageTable table = tableById.get(tableId);
        if (table == null) {
            return;
        }
        TableName tableInfo = TableName.valueOf(table.getNamespace(), table.getTableName(), tableId);
        _log.debug(tableInfo.getTableName() + " delete column id:{},name:{}", columnId, columnName);

        Mapping mapping = getMapping(tableId);
        MessageType schema = table.getSchema();
        if (schema == null) {
            TableType tableType = table.getRowMeta().getType();
            schema = schemaUtils.getSchema(tableInfo, mapping,tableType);
        }
        MessageType newSchema = MessageTypeSchemaUtils.removeFieldMessageType(schema, columnName);
        if (newSchema != null) {
            table.setSchema(newSchema);
        }
    }

    @Override
    public void postSchemaChange(SysMetaRow table, List<HColumnRow> columns) {
        if (table == null) {
            return;
        }
        if (columns == null || columns.size() == 0) {
            throw new OrcaObjectStoreException(table.getTableName() + " columns is empty");
        }
        TableName tableInfo = TableName.valueOf(table.getNamespace(), table.getTableName(), table.getTableId());
        MessageType newSchema = MessageTypeSchemaUtils.genSchema(tableInfo, columns);
        if (newSchema != null) {
            StorageTable cacheTable = tableById.get(table.getTableId());
            cacheTable.setSchema(newSchema);
        }
        updateDataAnalyticsTableSchema(tableInfo, columns);
    }

    public void updateDataAnalyticsTableSchema(TableName tableInfo, List<HColumnRow> columns) {}

    private void upload(UploadSet uploadSet,boolean wait) throws InterruptedException, IOException, ExecutionException {
        
        FishBoat boat = new FishBoat(this.getLocalDataHome(),
                this.getRemoteDataHome(),
                this.catchThreads,
                this.provider,
                uploadSet,
                this.getCommittedLogPointer(),
                this.checkPoint,
                this.obsCache
                );
        
        for (;;) {
            try {
                if (wait) {
                    Future<Boolean> future = this.boatThreads.getPool().submit(boat);
                    Boolean result = future.get();
                    _log.trace("boat success {}", result);
                }
                else {
                    this.boatThreads.getPool().submit(boat);
                }
                break;
            }
            catch (RejectedExecutionException e) {
               // _log.warn("boat full", e);
            }
            try {
                Thread.sleep(5000);
            }
            catch (InterruptedException ignored) {
            }
        }
    }
    
    private void recover(UploadSet uploadSet) throws InterruptedException, IOException, ExecutionException {
        FishBoat boat = new FishBoat(this.getLocalDataHome(),
                this.getRemoteDataHome(),
                this.catchThreads,
                this.provider,
                uploadSet,
                this.getCommittedLogPointer(),
                this.checkPoint,
                this.obsCache
                );
        
        for (;;) {
            try {
                Future<Boolean> future = this.boatThreads.getPool().submit(boat);
                Boolean result = future.get();
                _log.debug("recover boat success {}", result);
                break;
            }
            catch (RejectedExecutionException e) {
               // _log.warn("boat full", e);
            }
            try {
                Thread.sleep(5000);
            }
            catch (InterruptedException ignored) {
            }
        }
    }

    @Override
    public StorageTable getTable(int tableId) {
        return this.tableById.get(tableId);
    }

    @Override
    public StorageTable createTable(SysMetaRow meta) {
        String namespace = meta.getNamespace();
        namespace = namespace.equals(Orca.SYSNS) ? this.sysns : namespace;
        String tableName = meta.getTableName();
       
        TableName tableInfo = TableName.valueOf(namespace, tableName,  meta.getTableId());
        
        StorageTable table = new ParquetTable(this, 
                meta, 
                this.provider,
                this.obsCache,
                this.getLocalDataHome());
        PartitionIndex partitionIndex = table.getPartitionIndex();
        if(partitionIndex == null) {
            partitionIndex = new PartitionIndex(
                    this.getLocalDataHome(),
                    this.getObsCache(),
                    tableInfo,
                    this.getInitPartitiFile(meta.getTableId()));
        }
        Helper.createTable(
                partitionIndex,
                this.provider,
                this.getLocalDataHome(),
                this.getRemoteDataHome(),
                tableInfo);
        if(tableName.startsWith("x") && meta.getTableId()< 0x100) {
           MessageType schema = this.schemaUtils.getSystemSchema(tableInfo);
           table.setSchema(schema);
        }
        table.setPartitionIndex(partitionIndex);
        
        this.tableById.put(meta.getTableId(), table);
        return table;
    }

    @Override
    public boolean deleteTable(int tableId) {
        StorageTable table = this.tableById.get(tableId);
        if (table == null) {
            throw new OrcaObjectStoreException("StorageTable is null,tableId={}",tableId)  ;
        }
        table.setDelete(true);
        // sync delete athena table
        this.dropDataAnalyticsTableSchema(table.getNamespace(), table.getTableName());
        return true;
    }

    @Override
    public void syncTable(SysMetaRow meta) {
        if (this.tableById.get(meta.getTableId()) != null) {
            return;
        }
        StorageTable table = new ParquetTable(
                this, 
                meta, 
                this.provider,
                this.obsCache,
                this.getLocalDataHome());
        TableName tableInfo = TableName.valueOf(table.getNamespace(),table.getTableName(),meta.getTableId());
        _log.debug("sync table dbname={} tableId={} tableName={}",
                table.getNamespace(),
                meta.getTableId(),
                table.getTableName());
        if(table.getSchema() == null) {
            Mapping mapping = this.getMapping(meta.getTableId());
            _log.debug("sync table({}) schem is empty reload ...",tableInfo.getDatabaseTableNameAndId());
            TableType tableType = table.getRowMeta().getType();
            MessageType schema = this.schemaUtils.getSchema(tableInfo, mapping,tableType);
            table.setSchema(schema);
        }
        this.tableById.put(meta.getTableId(), table);
    }

    @Override
    public Replicable getReplicable() {
        return this.replicationHandler;
    }

    @Override
    public void close() throws Exception {
        if (this.isClosed) {
            return;
        }
        UberUtil.sleep(2 * 1000);
        this.isClosed = true;
        _log.info("obs storage is success closed");
    }

    protected void initParquetConfig(ConfigService antsdbConfig) throws Exception {
        this.md5Flag = antsdbConfig.getMd5Flag();
        partitionMaxDatasize = Integer.parseInt(antsdbConfig.getPartitionMaxDatasize()) * SizeConstants.MB;// unit M
        partitionMaxRowCount = Integer.parseInt(antsdbConfig.getPartitionMaxRowCount());
        if (partitionMaxRowCount == 0 && partitionMaxDatasize == 0) {
            partitionMaxDatasize = 50 * SizeConstants.MB;
        }
        String localData = antsdbConfig.getLocalData();

        this.localDataDirectory = new File(this.home,localData);

        long cacheCapacity = antsdbConfig.getObsCacheSize();
       
        this.obsCache = new ObsCache(
                cacheCapacity,
                this.provider,
                this.getLocalDataHome(),
                this.getRemoteDataHome(),
                this.isMutable);
    }

    protected void externalQueryConfigCheck(ConfigService antsdbConfig) throws IOException {
        if ("true".equalsIgnoreCase(antsdbConfig.getAthenaEnable())) {
            if (antsdbConfig.getS3accessKey() == null 
                    || antsdbConfig.getS3secretKey() == null
                    ) {
                throw new OrcaObjectStoreException("External query by athena config error!!!");
            }
        }
        if ("true".equalsIgnoreCase(antsdbConfig.getSparksqlEnable())) {
            String sparkHome = antsdbConfig.getSparkHome();
            String master = antsdbConfig.getSparkMaster();
            String deployMode = antsdbConfig.getSparkMode();
            String sparkTaskJar = antsdbConfig.getSparkTaskJar();
            String resultPath = antsdbConfig.getSparkResultPath();
            if (sparkHome == null 
                    || master == null 
                    || deployMode == null 
                    || sparkTaskJar == null
                    || resultPath == null) {
                throw new OrcaObjectStoreException("External query by sprak sql config error!!!");
            }
        }
    }

    public ObsProvider getStoreClient() {
        return this.provider;
    }

    @Override
    public void startMerge(TableName tableInfo, Group group, MessageType schema,TableType tableType) throws Exception {
       if(this.tableMerger == null) {
            StorageTable storageTable = this.getTable(tableInfo.getTableId());
            if(storageTable == null){
                throw new OrcaObjectStoreException("StorageTable is null,tableId={}",tableInfo.getTableId());
            }
            PartitionIndex partitionIndex = storageTable.getPartitionIndex();
            this.tableMerger = new TableMerger(
                    this.getLocalDataHome(), 
                    this.compressionType, 
                    partitionMaxDatasize, 
                    partitionMaxRowCount, 
                    tableInfo, 
                    schema,
                    this.obsCache,
                    partitionIndex,
                    md5Flag,
                    tableType);
       }
       else if(tableInfo.getTableId() != tableMerger.getTableId()) {// exception table change
           _log.warn("exception change table by {} -> {}",tableMerger.getTableId(),tableInfo.getTableId());
           StorageTable storageTable = this.getTable(tableInfo.getTableId());
           if(storageTable == null){
               throw new OrcaObjectStoreException("StorageTable is null,tableId={}",tableInfo.getTableId());
           }
           PartitionIndex partitionIndex = storageTable.getPartitionIndex();
           this.tableMerger = new TableMerger(
                   this.getLocalDataHome(), 
                   this.compressionType, 
                   partitionMaxDatasize, 
                   partitionMaxRowCount, 
                   tableInfo, 
                   schema,
                   this.obsCache,
                   partitionIndex,
                   md5Flag,
                   tableType);
       }
        
       this.tableMerger.merge(group);
    }
    
    @Override
    public void changeTableMerger(UploadSet uploadSet) throws Exception {
        if(this.tableMerger!=null) {
            this.tableMerger.closeCurrentTableMerge(uploadSet);

            this.tableMerger = null;
        }
    }

    @Override
    public void startCommit(long sp,UploadSet uploadSet) throws Exception {
        long syncLogPointer = this.getReplicateLogPointer();
        if (sp == 0 || sp >= syncLogPointer) {
            if (this.tableMerger != null) {
                _log.debug("have datas sync to store,sp update to :{} -> {}", 
                        syncLogPointer ,
                        sp);
                this.tableMerger.closeCurrentTableMerge(uploadSet);
                this.tableMerger = null;
                SyncParam syncParam = this.checkPoint.updateSyncParam(sp);
                if(syncParam!=null) {
                    ActionUploadSyncParam uploadSyncParam = getUploadSyncParam(true,syncParam);
                    uploadSet.setUploadSyncParam(uploadSyncParam);
                }
                else {
                    _log.warn("SyncParam is null");
                }
                upload(uploadSet,false);
                
                this.tableMerger = null;
            }
            else if (sp > syncLogPointer) {
                _log.debug("no datas sync to store,sp update to :{} -> {}", 
                        syncLogPointer ,
                        sp);
                SyncParam syncParam = this.checkPoint.updateSyncParam(sp);
                if(syncParam != null) {
                    UploadSet uploadSetTmp = new UploadSet();
                    ActionUploadSyncParam uploadSyncParam = getUploadSyncParam(false,syncParam);
                    uploadSetTmp.setUploadSyncParam(uploadSyncParam);
                    upload(uploadSetTmp,false);
                }
            }
            this.replicateLogPointer = sp;
        }
        else if (sp < syncLogPointer) {
            _log.error("error: lp {} is less than the one in storage {}", sp, syncLogPointer);
        }
        
    }

    @Override
    public void syncDropEntity(TableName delTableInfo ) {
        if(delTableInfo != null) {
            this.removeTableBytableId(delTableInfo);
        }
    }

    private void removeTableBytableId(TableName delTableInfo) {
        StorageTable table = this.tableById.get(delTableInfo.getTableId());
        if(table != null && table.isDelete()) {
            ConcurrentSkipListMap<byte[],Partition> indexs = table.getPartitions();
            if(indexs!=null) {
                for(Map.Entry<byte[],Partition> index : indexs.entrySet()) {
                    obsCache.remove(delTableInfo.getTablePath() + index.getValue().getVersionFileName() );
                }
            }
            this.tableById.remove(delTableInfo.getTableId());
        }
    }

    protected void checkAndRecoverBeforeSyncdata() throws Exception {
        String uploadSetPath = this.getRemoteDataHome() + FishBoat.REMOTE_TMP + UploadSet.UPLOADSET_FILENAME;
        boolean exists = this.provider.doesObjectExist(uploadSetPath);
        if(exists) {
            File tmpFile = new File(this.getLocalDataHome(), FishBoat.LOCAL_TMP);
            File uploadSetFile = new File(tmpFile, UploadSet.UPLOADSET_FILENAME);
            
            _log.debug("uploadSetPath: {}", uploadSetPath);
            _log.debug("exists: {}", exists);
            this.provider.downloadObject(uploadSetPath, uploadSetFile.getAbsolutePath());
            
            String contents = FileUtils.readFileToString(uploadSetFile);
            
            UploadSet uploadSet = UberUtil.toObject(contents, UploadSet.class);
            
            if(uploadSet != null) {
                recover(uploadSet);
            }
        }
    }

    protected void cleanLocalTempData() throws IOException {
        if (this.getLocalDataHome()!=null && this.getLocalDataHome().exists()) {
            FileUtils.deleteDirectory(this.getLocalDataHome());
        }
        FileUtils.forceMkdir(this.getLocalDataHome()); 
        boolean result = this.getLocalDataHome().exists();
        _log.trace("local data home :{},exists:{}",this.getLocalDataHome().getAbsolutePath(),result);
    }
    
    protected void init() throws Exception {
        
        checkAndRecoverBeforeSyncdata();
        
        cleanLocalTempData();
        
        loadAllParatitionIndex();
        // create antsdb namespaces and tables if they are missing
        setup();
        
        // load checkpoint
        this.checkPoint = new CheckPoint(TableName.valueOf(this.sysns, TABLE_SYNC_PARAM),this.isMutable);
        
        if (this.checkPoint.readSyncParam(this.obsCache) == null) {
            throw new OrcaObjectStoreException("obs check point is empty");
        }
        
        this.replicateLogPointer = this.checkPoint.getCurrentSp();
         
        _log.info("system strat CheckPoint currentSp is :{}",this.checkPoint.getCurrentSp());
        // load system tables
        List<TableName> tables = listTableNamesByDatabase( this.sysns);
        for (TableName i : tables) {
            //this.partitionUtils.reload(i);
            String name = i.getTableName();
            if (!name.startsWith("x") 
                    || name.endsWith(ParquetUtils.MERGE_PARQUET_EXT_NAME)) {
                continue;
            }
            int id = Integer.parseInt(name.substring(1), 16);
            SysMetaRow meta = new SysMetaRow(id);
            meta.setNamespace(Orca.SYSNS);
            meta.setTableName(name);
            meta.setType(TableType.DATA);
            StorageTable table = new ParquetTable(
                    this, 
                    meta, 
                    this.provider,
                    this.obsCache,
                    this.getLocalDataHome());
            PartitionIndex partitionIndex = new PartitionIndex(
                     getLocalDataHome(),
                     getObsCache(),
                     i,
                     getInitPartitiFile(i.getTableId())
                    );
            table.setPartitionIndex(partitionIndex);
            this.tableById.put(id, table);
        }

        // validations
        if (this.checkPoint.getServerId() != this.humpback.getServerId()) {
            throw new OrcaObjectStoreException(
                    "obs is currently linked to a different antsdb instance {},humpback is instance {}",
                    checkPoint.getServerId(), 
                    this.humpback.getServerId());
        }
        if (this.checkPoint.getCurrentSp() > this.humpback.getSpaceManager().getAllocationPointer()) {
            throw new OrcaObjectStoreException("obs synchronization pointer is ahead of local log");
        }
        
        // update checkpoint
        if (this.isMutable) {
            this.checkPoint.setActive(true);
            SyncParam syncParam = this.checkPoint.updateSyncParam();
            
            UploadSet uploadSet = new UploadSet();
            
            ActionUploadSyncParam uploadSyncParam = getUploadSyncParam(true,syncParam);
            uploadSet.setUploadSyncParam(uploadSyncParam);
             
            upload(uploadSet, true);
        }
    }

    private List<TableName> listTableNamesByDatabase(String namespace) throws Exception{
        String dbPath = this.getRemoteDataHome() + namespace;
        List<TableName> tables = new ArrayList<>();
        List<String> subDirectorys = provider.listDirectorys(dbPath);
        if(subDirectorys == null || subDirectorys.size() == 0) {
            return tables;
        }
        for(String fileNameTmp : subDirectorys) {
            _log.trace("database: {},table:{}",namespace,fileNameTmp);
            int index = fileNameTmp.indexOf("-");
            TableName table = null;
            if (index > 0) {
                String tableName = fileNameTmp.substring(0, index);
                
                if(fileNameTmp.endsWith("/")) {
                    fileNameTmp =  fileNameTmp.substring(0, fileNameTmp.length() - 1);
                }
                int lastIndex = fileNameTmp.lastIndexOf("-");
                int tableId = Integer.valueOf(fileNameTmp.substring(lastIndex + 1));
                
                table = TableName.valueOf(namespace, tableName, tableId);
            }
            else {
                String tableName = fileNameTmp;
                table = TableName.valueOf(namespace, tableName);
            }
            tables.add(table);
        }
        return tables;
    }
    
    private ActionUploadSyncParam getUploadSyncParam(boolean upload,SyncParam syncParam) throws Exception {
        TableName syncParamTable = TableName.valueOf(this.sysns, TABLE_SYNC_PARAM);
        String objectKey = syncParamTable.getTableName() + ParquetUtils.DATA_JSON_EXT_NAME;
        ActionUploadSyncParam uploadSyncParam = new ActionUploadSyncParam(upload,objectKey,syncParam);
        return uploadSyncParam;
    }
    
    private void loadAllParatitionIndex() throws Exception {
        List<String> databases = this.provider.listDirectorys(this.getRemoteDataHome());
        if(databases!=null && databases.size()>0) {
            for(String database : databases) {
                String databasePath = this.getRemoteDataHome() + database;
                List<String> tableIndexFiles = this.provider.listFiles(
                        databasePath, 
                        null, 
                        ParquetUtils.DATA_JSON_EXT_NAME);
                if(tableIndexFiles!=null) {
                    for(String tableIndex : tableIndexFiles) {
                        _log.debug("start load database={} table index={}",database,tableIndex);
                        if(tableIndex.endsWith("SYNCPARAM.json")) {
                            continue;
                        }
                        String tmpTableIndex = tableIndex.substring(tableIndex.lastIndexOf("/")+1);
                        Pattern r = Pattern.compile(PartitionIndex.PARTITION_PATTERN);
                        Matcher m = r.matcher(tmpTableIndex);
                        if (m.find( )) {
                            String tableIdHexStr = m.group(PartitionIndex.TABLEID);
                            _log.debug("parsing table file name={} tableId={} tableIdHexStr={}",
                                    tmpTableIndex,
                                    Bytes.toInt(Bytes.fromHex(tableIdHexStr)),
                                    tableIdHexStr
                                    );
                            int tableId = Bytes.toInt(Bytes.fromHex(tableIdHexStr));
                            String partitionIndexFileName = tmpTableIndex;
                            tablePartitionIndexs.put(tableId,partitionIndexFileName);
                        } 
                    }
                }
            }
        }
    }
    
    public String getInitPartitiFile(int tableId) {
        return tablePartitionIndexs.get(tableId);
    }

    @Override
    public ObsCache getObsCache() {
        return this.obsCache;
    }
    
    @Override
    public Collection<StorageTable> getTableInfos() {
        return this.tableById.values();
    }
    
    @Override
    public ObsProvider getObsProvider() {
        return this.provider;
    }
    
    @Override
    public void startBackup(String dest) {
        
        String destResource = dest;
        if(destResource == null || destResource.length() == 0) {
            destResource = SaltedFish.getInstance().getOrca().getConfig().get("antsdb_backup_dest");
        }
        if(destResource == null || destResource.length() == 0) {
            throw new OrcaObjectStoreException("dest resource config is null,Please config (antsdb_backup_dest) value!");
        }
        
        boolean exists;
        try {
            exists = this.provider.checkRootResourceExists(destResource);
            
            if(!exists) {
                throw new OrcaObjectStoreException("dest resource not exists,Please create it",destResource);
            }
            
            boolean available = this.provider.isEmpty(destResource);
            if(!available) {
                throw new OrcaObjectStoreException("dest resource {} not empty,Please confirm config !",destResource)  ;
            }
            
            this.boatThreads.getPool().pause();
            for(;;) {
                if(this.catchThreads.isIdle()) {
                    _log.debug("backup catch thread is idle start backup data to {}",destResource);
                    break;
                }
                else {
                    _log.debug("backup wait catch thread idle,sleep 5 seconds");
                    UberUtil.sleep(5 * 1000);
                }
            }
            this.provider.backup(destResource);
            _log.info("backup data to {} success,by time={}",destResource,UberTime.getTime());
        }
        catch (Exception e) {
            throw new OrcaObjectStoreException(e,"start backup error,{}",e.getMessage());
        }finally {
            this.boatThreads.getPool().resume();
        }
    }
}
