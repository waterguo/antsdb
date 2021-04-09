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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.obs.LocalFileUtils;
import com.antsdb.saltedfish.obs.action.ActionDeleteFile;
import com.antsdb.saltedfish.obs.action.ActionUploadFile;
import com.antsdb.saltedfish.obs.action.UploadSet;
import com.antsdb.saltedfish.obs.cache.ObsCache;
import com.antsdb.saltedfish.obs.cache.ObsFileReference;
import com.antsdb.saltedfish.parquet.OrcaObjectStoreException;
import com.antsdb.saltedfish.parquet.ParquetDataReader;
import com.antsdb.saltedfish.parquet.ParquetDataWriter;
import com.antsdb.saltedfish.parquet.PartitionIndex;
import com.antsdb.saltedfish.parquet.TableName;
import com.antsdb.saltedfish.parquet.bean.Partition;

public class TableMerger{
    static Logger _log = LoggerFactory.getLogger(PartitionMerger.class.getName() + ".sync-log");
    
    private PartitionMerger partitionMerger;

    private File homePath;

    private TableName tableInfo;
 
    private MessageType schema;
    private ObsCache obsCache;
    
    private boolean endFlagNumber = true;
    private CompressionCodecName compressionCodecName;
    private long maxSizeParttion = 10 * 1024 * 1024;
    private long maxRowCountPartition = 1000;
    private PartitionIndex partitionIndex;

    private List<ActionUploadFile> uploadFiles = new ArrayList<>();
    private List<ActionDeleteFile> deleteFiles = new ArrayList<>();

    private Partition curIndex = null; 

    private ConcurrentSkipListMap<byte[],Partition> mergePartitions ;
    private boolean md5Flag;
    
    private TableType tableType;
    
    public TableMerger(
            File loacalDataHome, 
            CompressionCodecName compressionCodecName, 
            long maxSizeParttion, 
            long maxRowCountPartition,
            TableName tableInfo, 
            MessageType schema,
            ObsCache obsCache,
            PartitionIndex partitionIndex,
            boolean md5Flag,
            TableType tableType
            ) {
        this.homePath = loacalDataHome;
        this.compressionCodecName = compressionCodecName;
        this.maxSizeParttion = maxSizeParttion;
        this.maxRowCountPartition = maxRowCountPartition;
        this.tableInfo = tableInfo;
        this.schema = schema;
        this.obsCache = obsCache;
        this.partitionIndex = partitionIndex;
        this.mergePartitions = this.partitionIndex.getPartitions().clone();
        this.md5Flag = md5Flag;
        this.tableType = tableType;
        uploadFiles = new ArrayList<>();
        deleteFiles = new ArrayList<>();
    }
 
    public void merge(Group group) throws Exception {
        if (group == null) {
            return;
        }

        MergerUtils.synclog(this.tableInfo,group,this.tableType,"table-merger");
        
        createPartitionMerger(group);

        partitionMerger.merge(group,this.curIndex);
        
    }
    
    private void createPartitionMerger(Group group) throws Exception {
        
        Partition indexFound =  getPartitionIndex(group);
        
        if (this.curIndex != null && !this.curIndex.equals(indexFound)) {// 切换索引文件,把原来的关掉
            _log.trace("{}\tbeforeIndex:{} ,change to index:{},endFlagNumber:{}", 
                    this.tableInfo.getTableNameAndId(),
                    curIndex.getId() , 
                    indexFound.getId() ,
                    endFlagNumber);
            if(partitionMerger != null) {
                partitionMerger.closeMerger(this.curIndex,this.uploadFiles,this.deleteFiles);
            }
            this.curIndex = indexFound;
            this.partitionMerger = new PartitionMerger(
                    this.homePath, 
                    this.compressionCodecName,
                    this.maxSizeParttion,
                    this.maxRowCountPartition,
                    this.partitionIndex,
                    this.mergePartitions,
                    this.tableInfo,
                    this.schema,
                    this.curIndex,
                    this.obsCache,
                    this.md5Flag,
                    this.tableType);
        }
        else if (this.curIndex == null || this.partitionMerger == null) {
            this.curIndex = indexFound;
            this.partitionMerger = new PartitionMerger(
                    this.homePath, 
                    this.compressionCodecName,
                    this.maxSizeParttion,
                    this.maxRowCountPartition,
                    this.partitionIndex,
                    this.mergePartitions,
                    this.tableInfo,
                    this.schema,
                    this.curIndex,
                    this.obsCache,
                    this.md5Flag,
                    this.tableType);
        }
        
        if(this.partitionMerger == null) {
            throw new OrcaObjectStoreException("{} partition merger init fail",this.tableInfo.getDatabaseTableNameAndId()) ; 
        }
    }

    private Partition getPartitionIndex(Group group) throws Exception {
        byte[] searchKey = MergerUtils.getKeyByGroup(group,this.tableType);
        if(searchKey == null) {
            throw new OrcaObjectStoreException("table={} name={} type={} group={}",
                    this.tableInfo.getTableId(),
                    this.tableInfo.getTableName(),
                    tableType,
                    MergerUtils.showGroupContent(group))  ; 
        }
        Partition indexFound = MergerUtils.getPartitionIndexByRowkey(
                this.mergePartitions,
                searchKey,
                this.maxRowCountPartition,
                this.maxSizeParttion);
        if (indexFound == null) {// no search index
            _log.trace("table:{},not found index create by key:{}", 
                    this.tableInfo.getTableNameAndId(),
                    Bytes.toHex(searchKey));
            indexFound = new Partition(
                    this.tableInfo.getDatabaseName(), 
                    this.tableInfo.getTableName(),
                    this.tableInfo.getTableId());
            indexFound.setId(this.partitionIndex.getNextIndexId());
            indexFound.setStartKey(searchKey);
            indexFound.setEndKey(Partition.defaultEndKey);
            indexFound.setStatus(PartitionIndex.STATUS_NODATA);
            indexFound.setRowCount(0L);
            indexFound.setDataSize(0L);
            MergerUtils.addPartition(this.mergePartitions,indexFound);
        }
        
        return indexFound;
    }

    public void closeCurrentTableMerge(UploadSet uploadSet) throws Exception {
        if(partitionMerger == null) {
            _log.warn("partitionMerger is null");
        }
        else {
            partitionMerger.closeMerger(this.curIndex,this.uploadFiles,this.deleteFiles);
            //partitionMerger = null;
        }
        
        MergerUtils.updatePartition(this.mergePartitions,this.curIndex);
        //压缩分区
        compressPartition();
        
        // add lock
        this.partitionIndex.setPartitions(this.mergePartitions);
        
        if (this.uploadFiles.size() > 0) {

            String oldVersionPartitionFileName = this.partitionIndex.getIndexFileVersionPath();
            ActionDeleteFile deleteFile = new ActionDeleteFile(oldVersionPartitionFileName);
            this.deleteFiles.add(deleteFile);

            ObsFileReference partitionIndexRef = this.partitionIndex.flush();
            
            if (partitionIndexRef != null) {
                ActionUploadFile uploadFile = new ActionUploadFile(partitionIndexRef,md5Flag);
                this.uploadFiles.add(uploadFile);
            }
            else {
                throw new OrcaObjectStoreException("{} partition index is null"
                        ,this.tableInfo.getDatabaseTableNameAndId()) ;
            }

            if (uploadSet.getUploadActions() != null) {
                for(ActionUploadFile uploadFile : this.uploadFiles) {
                    uploadSet.getUploadActions().add(uploadFile);
                }
            }
            this.uploadFiles.clear();
        }
        if (this.deleteFiles.size() > 0) {
            if (uploadSet.getDeleteActions() != null) {
                for(ActionDeleteFile deleteFile : this.deleteFiles) {
                    uploadSet.getDeleteActions().add(deleteFile);
                }
            }
            this.deleteFiles.clear();
        }
    }
        
    private void compressPartition() throws Exception {
        if (this.mergePartitions.size() > 2) {
            _log.trace("table={} partition merge before size={} rows={}",
                    this.tableInfo.getTableName(),
                    this.mergePartitions.size(),
                    MergerUtils.getRowsCount(this.mergePartitions));
            Map.Entry<byte[], Partition> temp = this.mergePartitions.firstEntry();
            for (;;) {
                byte[] startKey = temp.getKey();
                Partition tmpPartition = temp.getValue();
                _log.trace("compress partition table={} pId={} rowCount={} dataSize={} startKey={} ",
                        tmpPartition.getTableName(),
                        tmpPartition.getId(),
                        tmpPartition.getRowCount(),
                        tmpPartition.getDataSize(),
                        Bytes.toHex(startKey)
                        );
                
                long rowCount = tmpPartition.getRowCount();
                long dataSize = tmpPartition.getDataSize();
                if (rowCount == 0 
                        && dataSize == 0 
                        && Bytes.compareTo(startKey, Partition.defaultStartKey) == 0) {
                    Map.Entry<byte[], Partition> next = this.mergePartitions.higherEntry(startKey);
                    if (next == null) {
                        break;
                    }
                    else {
                        Partition nextPartition = next.getValue();
                        mergePartition(tmpPartition,nextPartition);
                    }
                }
                else if (rowCount == 0 
                        && dataSize == 0 
                        && Bytes.compareTo(startKey, Partition.defaultStartKey) != 0) {
                    String partitionFileName = tmpPartition.getVersionFileName();
                    String oldVersionPartitionFileName = tableInfo.getTablePath() + partitionFileName;
                    ActionDeleteFile deleteFile = new ActionDeleteFile(oldVersionPartitionFileName);
                    this.deleteFiles.add(deleteFile);
                    this.mergePartitions.remove(startKey);
                    
                    temp = this.mergePartitions.higherEntry(startKey);
                    if (temp == null) {
                        break;
                    }
                    continue;
                }
                else if ((this.maxRowCountPartition > 0 && rowCount < this.maxRowCountPartition / 2)
                        && (this.maxSizeParttion > 0 && dataSize < this.maxSizeParttion / 2)) {
                    Map.Entry<byte[], Partition> next = this.mergePartitions.higherEntry(startKey);
                    if (next == null) {
                        break;
                    }
                    else {
                        Partition nextPartition = next.getValue();
                        long nextRowCount = nextPartition.getRowCount();
                        long nextDataSize = nextPartition.getDataSize();
                        if ((this.maxRowCountPartition > 0 && nextRowCount < this.maxRowCountPartition / 2)
                                && (this.maxSizeParttion > 0 && nextDataSize < this.maxSizeParttion / 2)) {
                            // 合并
                            mergePartition(tmpPartition,nextPartition);
                        }
                        else {
                            startKey = next.getKey();
                            temp = this.mergePartitions.higherEntry(startKey);
                            if (temp == null) {
                                break;
                            }
                            continue;
                        }
                    }
                }
                else {
                    startKey = tmpPartition.getStartKey();
                    temp = this.mergePartitions.higherEntry(startKey);
                    if (temp == null) {
                        break;
                    }
                }
            }//end for
            _log.trace("table={} partition merge end size={} rows={}",
                    this.tableInfo.getTableName(),
                    this.mergePartitions.size(),
                    MergerUtils.getRowsCount(this.mergePartitions));
        }
    }
    
    private void mergePartition(Partition tmpPartition,Partition nextPartition) throws Exception {
        _log.trace("table={} merge partition one:{} other:{}",
                this.tableInfo.getTableName(),
                tmpPartition.toLog(),
                nextPartition.toLog());
        tmpPartition.setEndKey(nextPartition.getEndKey());
        
        String nextPartitionFileName = tableInfo.getTablePath()
                + nextPartition.getVersionFileName();
        
        ActionDeleteFile deleteFile = new ActionDeleteFile(nextPartitionFileName);
        this.deleteFiles.add(deleteFile);
        _log.trace("table={} merge partition delete:{}",this.tableInfo.getTableName(),nextPartition.toLog());
        this.mergePartitions.remove(nextPartition.getStartKey());
        
        String mergeFileName = this.getFilePath(this.homePath, this.tableInfo.getTablePath(),
                tmpPartition.getMergeFileName());
        
        long rowCount = tmpPartition.getRowCount();
        long dataSize = tmpPartition.getDataSize();
        long nextRowCount = nextPartition.getRowCount();
        long nextDataSize = nextPartition.getDataSize();
        
        if(rowCount > 0 
                || dataSize > 0
                || nextRowCount > 0 
                || nextDataSize > 0) {
            try (ParquetDataWriter writer = new ParquetDataWriter(
                    mergeFileName,
                    schema,
                    this.compressionCodecName, 
                    ParquetFileWriter.Mode.CREATE)) {

                long newRowCount = 0;
                if(rowCount > 0 || dataSize > 0) {
                    long tmpNewRowCount = syncData(writer, tmpPartition);
                    newRowCount += tmpNewRowCount;
                    tmpPartition.setRowCount(newRowCount);
                    tmpPartition.setDataSize(writer.getDataSize());
                }
                if(nextRowCount > 0 || nextDataSize > 0) {
                    long tmpNewRowCount = syncData(writer, nextPartition);
                    newRowCount += tmpNewRowCount;
                    tmpPartition.setRowCount(newRowCount);
                    tmpPartition.setDataSize(writer.getDataSize());
                }
                _log.trace("table={} merger partition complate rowcount={} dataSize={} partition info={}",
                        this.tableInfo.getTableName(),
                        newRowCount,
                        writer.getDataSize(),
                        tmpPartition.toLog());
            }
            String partitionFileName = tmpPartition.getVersionFileName();
            String oldVersionPartitionFileName = this.tableInfo.getTablePath() + partitionFileName;

            String versionNewFname = this.getFilePath(
                    this.homePath,
                    this.tableInfo.getTablePath(),
                    tmpPartition.getNextVersionFileName()
                    );

            boolean result = LocalFileUtils.renameHdfs(mergeFileName, versionNewFname);
            _log.trace("table={} merger partition rename {}->{},result={}",
                    this.tableInfo.getTableName(),
                    mergeFileName,
                    versionNewFname,
                    result);
            ActionDeleteFile deleteOldVersionFile = new ActionDeleteFile(oldVersionPartitionFileName);
            this.deleteFiles.add(deleteOldVersionFile);
            
            
            String objectKey = tableInfo.getTablePath() + tmpPartition.getVersionFileName();
            ObsFileReference fileCache = new ObsFileReference(objectKey, new File(versionNewFname),"merge partition");

            ActionUploadFile uploadFile = new ActionUploadFile(fileCache, md5Flag);
            this.uploadFiles.add(uploadFile);
            this.obsCache.put(fileCache);
            _log.trace("table={} merger partition over delete={} upload={}->{}",
                    this.tableInfo.getTableName(),
                    deleteOldVersionFile.getObjectKey(),
                    versionNewFname,
                    objectKey); 
        }
        else {
            _log.trace("table={} merger partition cur(row={} size={}) and next(row={} size={}) partition all is empty",
                    this.tableInfo.getTableName(),
                    rowCount,
                    dataSize,
                    nextRowCount,
                    nextDataSize);
        }

        MergerUtils.updatePartition(this.mergePartitions, tmpPartition);
    }
    
    private long syncData(ParquetDataWriter writer,Partition tmpPartition) throws Exception {
        long rowCount = 0;
        String tmpObjectKey = tableInfo.getTablePath() + tmpPartition.getVersionFileName();
        ObsFileReference parquetFile = this.obsCache.get(tmpObjectKey);
        
        if (parquetFile !=null ) {
            File localFile = parquetFile.getFile();
            if(localFile!=null && localFile.exists()) {
                String tempPath = localFile.getAbsolutePath();
                try(ParquetDataReader reader = new ParquetDataReader(new Path(tempPath), new Configuration())){
                    Group data = null;
                    while ((data = reader.readNext()) != null) {
                        writer.writeData(data);
                        rowCount ++;
                    }
                }
            } 
        }
        return rowCount;
    }
    
    private String getFilePath(File baseHomePath, String tablePath, String versionFileName) {
        File tablePathObj = new File(baseHomePath, tablePath);
        File fullFile = new File(tablePathObj, versionFileName);
        return fullFile.getAbsolutePath();
    }
    
    public int getTableId() {
        return tableInfo.getTableId();
    }
    
}
