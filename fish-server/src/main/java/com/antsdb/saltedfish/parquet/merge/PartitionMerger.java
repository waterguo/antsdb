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
import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.obs.LocalFileUtils;
import com.antsdb.saltedfish.obs.action.ActionDeleteFile;
import com.antsdb.saltedfish.obs.action.ActionUploadFile;
import com.antsdb.saltedfish.obs.cache.ObsCache;
import com.antsdb.saltedfish.obs.cache.ObsFileReference;
import com.antsdb.saltedfish.parquet.MessageTypeSchemaUtils;
import com.antsdb.saltedfish.parquet.ParquetDataReader;
import com.antsdb.saltedfish.parquet.ParquetDataWriter;
import com.antsdb.saltedfish.parquet.PartitionIndex;
import com.antsdb.saltedfish.parquet.TableName;
import com.antsdb.saltedfish.parquet.bean.Partition;
import com.antsdb.saltedfish.util.UberUtil;

public class PartitionMerger {

    static Logger _log = UberUtil.getThisLogger();

    private File homePath;
    private TableName tableInfo;
    private MessageType schema;
    private Group current;
    private boolean endFlagNumber = true;
    private CompressionCodecName compressionCodecName;
    private long maxSizeParttion = 10 * 1024 * 1024;
    private long maxRowCountPartition = 1000;
    
    private ParquetDataReader reader;

    private boolean schemaChange = false;

    private PartitionIndex partitionUtils;
    ConcurrentSkipListMap<byte[],Partition> mergePartitions;
    private ParquetDataWriter writerMerge = null;

    private ObsCache obsCache;

    private List<ActionDeleteFile> deleteFiles = new ArrayList<>();
    private ConcurrentSkipListMap<String,ActionUploadFile> uploadFiles = new ConcurrentSkipListMap<>(
            new Comparator<String>(){
                @Override
                public int compare(String o1, String o2) {
                    return o1.compareToIgnoreCase(o2);
                }
            } 
        );
    
    private int newdataCount = 0;
    private int readataCount = 0;
    private int insertCount = 0;
    private int updateCount = 0;
    private int deleteCount = 0;
    private boolean md5Flag;
    private TableType tableType;
    private Group minData;// 当前分区的最小值
    private Group maxData;// 当前分区的最大值
    

    public PartitionMerger(File homePath, 
            CompressionCodecName compressionCodecName, 
            long maxSizeParttion,
            long maxRowCountPartition,
            PartitionIndex partitionUtils,
            ConcurrentSkipListMap<byte[],Partition> mergePartitions,
            TableName tableInfo,
            MessageType schema, 
            Partition index, 
            ObsCache obsCache,
            boolean md5Flag,
            TableType tableType
            ) throws IOException, Exception {
        this.homePath = homePath;
        this.compressionCodecName = compressionCodecName;
        this.maxSizeParttion = maxSizeParttion;
        this.maxRowCountPartition = maxRowCountPartition;
        this.partitionUtils = partitionUtils;
        this.mergePartitions = mergePartitions;
        this.tableInfo = tableInfo;
        this.schema = schema;

        this.obsCache = obsCache;
        this.md5Flag = md5Flag;
        this.tableType = tableType;
        
        initPrtitionMerger(index);
        
    }
    
    public void closeCurrentPartition(Partition index) throws Exception {

        if (writerMerge != null && !writerMerge.isClose()) {
            writerMerge.close();
            writerMerge = null;

            String partitionFileName = index.getMergeFileName();
            String mergeFileName = this.getFilePath(homePath, this.tableInfo.getTablePath(), partitionFileName);

            mergeAndUpload(tableInfo, mergeFileName, index, "call-closeCurrentPartition-1", schema);
        }
        if (!endFlagNumber) {
            sourceAfterDataProc(tableInfo, schema);
        }

    }

    public void closeMerger(Partition index, List<ActionUploadFile> parentUploadFiles,
            List<ActionDeleteFile> parentDeleteFiles) throws Exception {
        if (!endFlagNumber) {
            sourceFileDataProc(tableInfo, schema, index);
        }

        closeReader();

        if (writerMerge != null && !writerMerge.isClose()) {
            writerMerge.close();
            writerMerge = null;

            String partitionFileName = index.getMergeFileName();
            String mergeFileName = this.getFilePath(homePath, this.tableInfo.getTablePath(), partitionFileName);

            mergeAndUpload(tableInfo, mergeFileName, index, "call-dataCollection", this.schema);

        }
        MergerUtils.updateIndexStatus(index);
        MergerUtils.updateStartKey(index, this.minData,this.tableType);
        MergerUtils.updateEndKey(index, this.maxData,this.tableType);
        
        if (this.uploadFiles.size() > 0) {
            for(ActionUploadFile action:this.uploadFiles.values()) {
                if(!parentUploadFiles.contains(action)) {
                    parentUploadFiles.add(action);
                }
            }
            this.uploadFiles.clear();
        }
        if (this.deleteFiles.size() > 0) {
            for(ActionDeleteFile action:this.deleteFiles) {
                if(!parentDeleteFiles.contains(action)) {
                    parentDeleteFiles.add(action);
                }
            }
            this.deleteFiles.clear();
        }
        if(_log.isTraceEnabled()) {
            NumberFormat numberFormat = NumberFormat.getInstance();   
            numberFormat.setMaximumFractionDigits(2);   
            String result = "";
            if(readataCount > 0) {
                result = numberFormat.format((float)newdataCount/(float)readataCount*100);
            }
            _log.trace("partition sync new data:{},read data:{},percentage:{}",newdataCount,readataCount,result);
        }
    }

    public void merge(Group group, Partition index) throws Exception {
        
        initPrtitionMerger(index);

        if (endFlagNumber) {// 表示原文件中没有数据，直接把新数据定入
            minData = MergerUtils.updateMinGroup(minData, group, tableType);
            maxData = MergerUtils.updateMaxGroup(maxData, group, tableType, 1);
            if (!MergerUtils.isDeletedData(group)) {
                MergerUtils.synclog(this.tableInfo,group,this.tableType,"partition-merger1");
                writerMerge.writeData(group);
                index.setRowCount(writerMerge.getRowCount());
                index.setDataSize(writerMerge.getDataSize());
                this.insertCount ++;
            }
            else {
                this.deleteCount ++;
            }
        }
        else {
            syncSourceData(group, index, schema);
        } // end if else
        this.newdataCount ++;
        
        // update end key of the current partition
        MergerUtils.updateStartKey(index,minData,this.tableType);
        MergerUtils.updateEndKey(index,maxData,this.tableType);
        MergerUtils.updatePartition(this.mergePartitions,index);
        
        // if current partition is full, close and create new one
        if (MergerUtils.checkPatition(
                this.maxSizeParttion,
                this.maxRowCountPartition,
                index.getDataSize(), 
                index.getRowCount(), 
                this.tableInfo)) {
            if (minData != null) {
                MergerUtils.updateStartKey(index,minData,this.tableType);
            }
            if (maxData != null) {
                byte[] rowKey = MergerUtils.getKeyByGroup(maxData,this.tableType);
                index.setEndKey(rowKey);
            }
            
            closeCurrentPartition(index);
            //partitionMerger = null;
            // 刷新内存的index
            MergerUtils.updateIndexStatus(index);
            MergerUtils.updatePartition(this.mergePartitions,index);            
        }
    }

    private void initReader(Partition index, TableName tableInfo, MessageType schema)
            throws IOException, Exception {

        if (this.reader != null) {
            this.reader.close();
            this.reader = null;
            this.schemaChange = false;
        }

        String objectKey = tableInfo.getTablePath() + index.getVersionFileName();
        ObsFileReference parquetFile = this.obsCache.get(objectKey);
         
        if (parquetFile !=null ) {
            File localFile = parquetFile.getFile();
            if(localFile!=null && localFile.exists()) {
                String localPath = localFile.getAbsolutePath();
                this.reader = new ParquetDataReader(new Path(localPath), new Configuration());
            }
            else {
                this.reader = null;
            }   
        }
        else {
            this.reader = null;
        }
        if (this.reader != null) {
            next();
            MessageType fileSchema = this.reader.getSchema();
            boolean result = MessageTypeSchemaUtils.equalsMessageType(schema, fileSchema);
            if (!result) {
                _log.trace("table:{} schema change,new schema:{},file schema:{}",
                        tableInfo.getTableNameAndId(),
                        schema,
                        fileSchema);
                this.schemaChange = true;
            }
        }
        else {
            this.endFlagNumber = true;
        }
    }

    private void next() throws IOException {
        Group temp = null;
        if ((temp = reader.readNext()) != null) {
            this.current = temp;
            this.endFlagNumber = false;
            this.readataCount ++;
        }
        else {
            this.endFlagNumber = true;
        }
    }

    private void closeReader() throws Exception {
        if (reader != null) {
            reader.close();
            reader = null;
            schemaChange = false;
        }
    }

    /**
     * sync source file data
     * @param group
     * @param indexSearch
     * @param schema
     * @throws IOException
     */
    private void syncSourceData(Group group, Partition indexSearch, MessageType schema) throws IOException {
        MergerUtils.synclog(this.tableInfo,group,this.tableType,"partition-merger2");
        while (true) {// 原文件比当前新记录小的数据写入merger文件
            if (endFlagNumber) {
                if (!MergerUtils.isDeletedData(group)) {
                    minData = MergerUtils.updateMinGroup(minData, group, tableType);
                    maxData = MergerUtils.updateMaxGroup(maxData, group, tableType, 2);
                    
                    writerMerge.writeData(group);
                    indexSearch.setRowCount(writerMerge.getRowCount());
                    indexSearch.setDataSize(writerMerge.getDataSize());
                    this.insertCount ++;
                }
                else {
                    this.deleteCount ++;
                }
                break;
            }
            int res = compareTo(group, this.current);// 如果新数据大于原有数据，把原有数据加入缓存
            if (res > 0) {// group big
                minData = MergerUtils.updateMinGroup(minData, group, tableType);
                maxData = MergerUtils.updateMaxGroup(maxData, current, tableType, 3);
                
                Group tmp = reorganizeGroup(this.current, schema);
                writerMerge.writeData(tmp);
                indexSearch.setRowCount(writerMerge.getRowCount());
                indexSearch.setDataSize(writerMerge.getDataSize());

                this.current = null;
                next();
            }
            else if (res == 0) {
                if (!MergerUtils.isDeletedData(group)) {
                    minData = MergerUtils.updateMinGroup(minData, group, tableType);
                    maxData = MergerUtils.updateMaxGroup(maxData, group, tableType, 4);
                    MergerUtils.synclog(this.tableInfo,group,this.tableType,"partition-merger-write3");
                    this.updateCount ++;
                    writerMerge.writeData(group);
                    indexSearch.setRowCount(writerMerge.getRowCount());
                    indexSearch.setDataSize(writerMerge.getDataSize());
                }
                else {
                    this.deleteCount ++;
                    indexSearch.setRowCount(writerMerge.getRowCount());
                    indexSearch.setDataSize(writerMerge.getDataSize());
                }

                this.current = null;
                next();
                break;
            }
            else {// group small
                if (!MergerUtils.isDeletedData(group)) {
                    minData = MergerUtils.updateMinGroup(minData, group, tableType);
                    maxData = MergerUtils.updateMaxGroup(maxData, group, tableType, 5);
                    MergerUtils.synclog(this.tableInfo,group,this.tableType,"partition-merger-write4");
                    this.insertCount ++;
                    writerMerge.writeData(group);
                    indexSearch.setRowCount(writerMerge.getRowCount());
                    indexSearch.setDataSize(writerMerge.getDataSize());
                }
                else {
                    this.deleteCount ++;
                }
                break;
            }
        }
    }

    private int compareTo(Group group, Group otherGroup) {
        if (group == null && otherGroup == null) {
            return 0;
        }
        if (group == null && otherGroup != null) {
            return -1;
        }
        if (group != null && otherGroup == null) {
            return 1;
        }       
        
        byte[] localRowkey =  MergerUtils.getKeyByGroup(group,this.tableType);
        byte[] otherRowkey =  MergerUtils.getKeyByGroup(otherGroup,this.tableType);
        int result = Bytes.compareTo(localRowkey, otherRowkey);
        return result;
    }

    private Group reorganizeGroup(Group data, MessageType schema) {
        if (schemaChange) {
            return MessageTypeSchemaUtils.copyGroupByMessageType(data, schema);
        }
        else {
            return data;
        }
    }

    private String getFilePath(File baseHomePath, String tablePath, String versionFileName) {
        File tablePathObj = new File(baseHomePath, tablePath);
        File fullFile = new File(tablePathObj, versionFileName);
        return fullFile.getAbsolutePath();
    }

    private void mergeAndUpload(TableName tableInfo, String mergeFileName, Partition index, String call,
            MessageType schema) throws IOException {

        boolean exists = LocalFileUtils.existsLocal(mergeFileName);
        long fsize = LocalFileUtils.getFileSizeLocal(mergeFileName);

        _log.trace("mergeAndUpload:{},call:{},exists:{},fsize:{}", mergeFileName, call, exists, fsize);
        if (exists && fsize > ParquetDataWriter.minParquetSize) {
            String partitionFileName = index.getVersionFileName();

            String oldVersionPartitionFileName = this.tableInfo.getTablePath() + partitionFileName;
            String nextVersionFileName = index.getNextVersionFileName();
            String versionNewFname = this.getFilePath(
                    this.homePath, 
                    this.tableInfo.getTablePath(),
                    nextVersionFileName);
            while (LocalFileUtils.existsLocal(versionNewFname)) {
                nextVersionFileName = index.getNextVersionFileName();
                versionNewFname = this.getFilePath(
                        this.homePath, 
                        this.tableInfo.getTablePath(),
                        nextVersionFileName);
            }
            boolean result = LocalFileUtils.renameHdfs(mergeFileName, versionNewFname);
            _log.trace("mergeAndUpload rename {}->{},result:{}",
                    mergeFileName,
                    versionNewFname,
                    result);
            if(_log.isDebugEnabled()) {
                String startKey = Bytes.toHex(index.getStartKey());
                String endKey = Bytes.toHex(index.getEndKey());
                long count = index.getRowCount();
                long dataSize = index.getDataSize();
                File f = new File(versionNewFname);
                long fileSize = f.length();
                _log.debug("file={}({}) generated startKey={} endKey={} count={} size={} fsize={} insert={} update={} delete={} newData={} call={}",
                        index.getVersionFileName(),
                        nextVersionFileName,
                        startKey,
                        endKey,
                        count,
                        dataSize,
                        fileSize,
                        insertCount,
                        updateCount,
                        deleteCount,
                        this.newdataCount,
                        call
                        );
            }
            if (result) {
                ActionDeleteFile deleteFile = new ActionDeleteFile(oldVersionPartitionFileName);
                this.deleteFiles.add(deleteFile);
                this.uploadFiles.remove(oldVersionPartitionFileName); 
                
                String objectKey = tableInfo.getTablePath() + index.getVersionFileName();
                ObsFileReference fileCache = new ObsFileReference(objectKey, new File(versionNewFname),"merge");
                if(fileCache!=null && !fileCache.getFile().exists()) {
                    _log.warn("mergeAndUpload upload {} file not exists",versionNewFname);
                }

                this.obsCache.put(fileCache);
                
                ActionUploadFile uploadFile = new ActionUploadFile(fileCache,this.md5Flag);
                this.uploadFiles.put(fileCache.getKey(),uploadFile);
            }
        }
        else {
            String partitionFileName = index.getVersionFileName();
            String oldVersionPartitionFileName = this.tableInfo.getTablePath() + partitionFileName;

            ActionDeleteFile deleteFile = new ActionDeleteFile(oldVersionPartitionFileName);
            this.deleteFiles.add(deleteFile);
            this.uploadFiles.remove(oldVersionPartitionFileName);
            
            LocalFileUtils.deleteFileLocal(mergeFileName);
        }
    }
    
    /**
     * fun 当前分区满了关闭后，处理原文件中的剩余数据。
     * @param tableInfo
     * @param schema
     * @throws Exception
     */
    private void sourceAfterDataProc(TableName tableInfo, MessageType schema) throws Exception {
        Group firstData = null;
        Group lastData = null;
        while (!endFlagNumber) {
            // create new index,写入原文件中溢出部分数据
            Partition overflowIndex = new Partition(
                    tableInfo.getDatabaseName(), 
                    tableInfo.getTableName(),
                    tableInfo.getTableId());
            byte[] startKey = MergerUtils.getKeyByGroup(this.current,this.tableType);

            overflowIndex.setId(partitionUtils.getNextIndexId());
            overflowIndex.setStartKey(startKey);
            overflowIndex.setStatus(PartitionIndex.STATUS_NEW);
            overflowIndex.setEndKey(Partition.defaultEndKey);
            overflowIndex.setRowCount(0L);
            overflowIndex.setDataSize(0L);

            MergerUtils.addPartition(this.mergePartitions,overflowIndex);

            String partitionFileName = overflowIndex.getVersionFileName();

            String overflowFullFileName = this.getFilePath(homePath, this.tableInfo.getTablePath(), partitionFileName);

            ParquetDataWriter writer = new ParquetDataWriter(
                    overflowFullFileName, 
                    schema, 
                    this.compressionCodecName,
                    ParquetFileWriter.Mode.CREATE);
            while (!endFlagNumber) {
                writer.writeData(reorganizeGroup(this.current, schema));
                overflowIndex.setRowCount(overflowIndex.getRowCount() + 1);
                overflowIndex.setDataSize(writer.getDataSize());
                firstData = MergerUtils.updateMinGroup(firstData, current, tableType);
                lastData = MergerUtils.updateMaxGroup(lastData, this.current,tableType,100);
                this.current = null;
                next();
            }
            if (writer != null) {
                writer.close();
                writer = null;
                MergerUtils.updateStartKey(overflowIndex, firstData,this.tableType);
                MergerUtils.updateEndKey(overflowIndex, lastData,this.tableType);

                String localPath = overflowFullFileName;

                String objectKey = tableInfo.getTablePath() + partitionFileName;

                ObsFileReference fileCache = new ObsFileReference(objectKey, new File(localPath),"merge");

                if(_log.isDebugEnabled()) {
                    String startKeyHex = Bytes.toHex(overflowIndex.getStartKey());
                    String endKeyHex = Bytes.toHex(overflowIndex.getEndKey());
                    long count = overflowIndex.getRowCount();
                    long dataSize = overflowIndex.getDataSize();
                    File f = new File(overflowFullFileName);
                    long fileSize = f.length();
                    _log.debug("line 543 file={} generated startKey={} endKey={} count={} size={} fsize={} insert={} update={} delete={} newData={}",
                            overflowIndex.getVersionFileName(),
                            startKeyHex,
                            endKeyHex,
                            count,
                            dataSize,
                            fileSize,
                            insertCount,
                            updateCount,
                            deleteCount,
                            this.newdataCount
                            );
                }
                
                ActionUploadFile uploadFile = new ActionUploadFile(fileCache,this.md5Flag);
                this.uploadFiles.put(fileCache.getKey(),uploadFile);
                
                obsCache.put(fileCache);
            }
            MergerUtils.updateIndexStatus(overflowIndex);
            MergerUtils.updatePartition(this.mergePartitions, overflowIndex);
        }
    }

    private void sourceFileDataProc(TableName tableInfo, MessageType schema, Partition curIndex) throws Exception {
        Group firstData = null;
        Group lastData = null;
        while (!endFlagNumber) {
            if (writerMerge != null && !writerMerge.isClose()) {
                writerMerge.writeData(reorganizeGroup(this.current, schema));
                curIndex.setDataSize(writerMerge.getDataSize());
                curIndex.setRowCount(writerMerge.getRowCount());

                MergerUtils.updatePartition(this.mergePartitions,curIndex);
                firstData = MergerUtils.updateMinGroup(firstData, current, tableType);
                lastData = MergerUtils.updateMaxGroup(lastData, this.current,this.tableType,101);

                if (MergerUtils.checkPatition(
                        this.maxSizeParttion,
                        this.maxRowCountPartition,
                        curIndex.getDataSize(), 
                        curIndex.getRowCount(), 
                        tableInfo)) {

                    MergerUtils.updateEndKey(curIndex, lastData,this.tableType);

                    if (writerMerge != null && !writerMerge.isClose()) {
                        writerMerge.close();
                        writerMerge = null;
                        if (lastData != null) {
                            MergerUtils.updateStartKey(curIndex, firstData, tableType);
                        }
                        if (lastData != null) {
                            byte[] rowKey = MergerUtils.getKeyByGroup(lastData, this.tableType);
                            curIndex.setEndKey(rowKey);
                        }

                        String partitionFileName = curIndex.getMergeFileName();
                        String mergeFileName = this.getFilePath(homePath, this.tableInfo.getTablePath(),
                                partitionFileName);

                        mergeAndUpload(tableInfo, mergeFileName, curIndex, "call-sourceFileDataProc-1", schema);
                    }
                    MergerUtils.updatePartition(this.mergePartitions,curIndex);
                }
                next();
            }
            else {
                // create new index,写入原文件中溢出部分数据
                Partition overflowIndex = new Partition(
                        tableInfo.getDatabaseName(), 
                        tableInfo.getTableName(),
                        tableInfo.getTableId());

                byte[] startKey = MergerUtils.getKeyByGroup(this.current,this.tableType);
                
                overflowIndex.setId(partitionUtils.getNextIndexId());
                overflowIndex.setStartKey(startKey);
                overflowIndex.setStatus(PartitionIndex.STATUS_NEW);
                overflowIndex.setEndKey(Partition.defaultEndKey);
                overflowIndex.setRowCount(0L);
                overflowIndex.setDataSize(0L);

                MergerUtils.addPartition(this.mergePartitions,overflowIndex);

                String partitionFileName = overflowIndex.getVersionFileName();

                String overflowFullFileName = this.getFilePath(homePath, this.tableInfo.getTablePath(),
                        partitionFileName);

                ParquetDataWriter writer = new ParquetDataWriter(
                        overflowFullFileName, 
                        schema,
                        this.compressionCodecName, 
                        ParquetFileWriter.Mode.CREATE);
                while (!endFlagNumber) {
                    writer.writeData(reorganizeGroup(this.current, schema));
                    overflowIndex.setRowCount(overflowIndex.getRowCount() + 1);
                    overflowIndex.setDataSize(writer.getDataSize());
                    firstData = MergerUtils.updateMinGroup(firstData, current, tableType);
                    lastData = MergerUtils.updateMaxGroup(lastData, this.current,this.tableType,102);
                    this.current = null;
                    next();
                }
                if (writer != null) {
                    writer.close();
                    writer = null;
                    
                    MergerUtils.updateStartKey(overflowIndex, firstData,this.tableType);
                    MergerUtils.updateEndKey(overflowIndex, lastData,this.tableType);
                    String localPath = overflowFullFileName;
                    String objectKey = tableInfo.getTablePath() + partitionFileName;
                    ObsFileReference fileCache = new ObsFileReference(objectKey, new File(localPath),"merge");
                    
                    if(_log.isDebugEnabled()) {
                        String startKeyHex = Bytes.toHex(overflowIndex.getStartKey());
                        String endKeyHex = Bytes.toHex(overflowIndex.getEndKey());
                        long count = overflowIndex.getRowCount();
                        long dataSize = overflowIndex.getDataSize();
                        File f = new File(overflowFullFileName);
                        long fileSize = f.length();
                        _log.debug("line 606 file={} generated startKey={} endKey={} count={} size={} fsize={} insert={} update={} delete={} newData={}",
                                overflowIndex.getVersionFileName(),
                                startKeyHex,
                                endKeyHex,
                                count,
                                dataSize,
                                fileSize,
                                insertCount,
                                updateCount,
                                deleteCount,
                                this.newdataCount
                                );
                    }
                    
                    ActionUploadFile uploadFile = new ActionUploadFile(fileCache,this.md5Flag);
                    this.uploadFiles.put(fileCache.getKey(),uploadFile);
                    
                    obsCache.put(fileCache);
                }
                MergerUtils.updateIndexStatus(overflowIndex);
                MergerUtils.updatePartition(this.mergePartitions,overflowIndex);
            }
        }
        closeReader();
    }

    public void initPrtitionMerger(Partition index) throws IOException, Exception {
        if(writerMerge == null || writerMerge.isClose()) {
            String mergeFileName = this.getFilePath(homePath, this.tableInfo.getTablePath(), index.getMergeFileName());
    
            _log.debug("table={} create merge file : {}", 
                    this.tableInfo.getTableNameAndId(), 
                    mergeFileName);
            writerMerge = new ParquetDataWriter(
                    mergeFileName, 
                    schema, 
                    this.compressionCodecName,
                    ParquetFileWriter.Mode.CREATE);
        }
        if ((index.getStatus() != PartitionIndex.STATUS_NODATA) 
                && (reader == null || reader.isClosed())) {
            initReader(index, this.tableInfo, schema);
        }
    }
}
