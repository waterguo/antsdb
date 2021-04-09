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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;

import com.antsdb.saltedfish.obs.cache.ObsCache;
import com.antsdb.saltedfish.obs.cache.ObsFileReference;
import com.antsdb.saltedfish.obs.hdfs.HdfsStorageService;
import com.antsdb.saltedfish.parquet.bean.Partition;
import com.antsdb.saltedfish.parquet.bean.PartitionIndexWarp;
import com.antsdb.saltedfish.util.UberTime;
import com.antsdb.saltedfish.util.UberUtil;

public class PartitionIndex {
    static Logger _log = UberUtil.getThisLogger();
    
    public static final int STATUS_NODATA = 0;
    public static final int STATUS_NEW = 1;// value 0,no data;1:NEW ;2,update;3,sync;9,delete
    public static final int STATUS_UPDATE = 2;
    public static final int STATUS_SYNC = 3;
    public static final int STATUS_DEL = 9;

    public static final String FILENAME_SUFFIX = ".json";
    public static final String PATITION_INDEX_FORMAT = "%s-%08x-%08x%s";// <table name>-<table id>-<version>.json
    
    
    public static final  String VERSION="version";
    public static final  String TABLEID="tableId";
    //"^\\S+-([0123456789abcdef]{8})-([0123456789abcdef]{8})";
    public static final String PARTITION_PATTERN = "^\\S+-(?<tableId>[0123456789abcdef]{8})-(?<version>[0123456789abcdef]{8})(\\.json){1}$";

    ConcurrentSkipListMap<byte[],Partition> partitions = new ConcurrentSkipListMap<>(
                new Comparator<byte[]>(){
                    @Override
                    public int compare(byte[] o1, byte[] o2) {
                        return Bytes.compareTo(o1, o2);
                    }
                } 
            );

    AtomicLong patitionId;
    AtomicLong patitionVersion;// partition file version
    
    private File localPath;
 
    private ObsCache obsCache;
    public TableName tableInfo;
    
    private Boolean lock = true;
    
    public ConcurrentSkipListMap<byte[],Partition> getPartitions() {
        synchronized (lock) {
            return partitions;
        }
    }
    
    public String getIndexFileName(long version) {
        return String.format(PATITION_INDEX_FORMAT, 
                this.tableInfo.getTableName(),
                this.tableInfo.getTableId(), 
                version,
                FILENAME_SUFFIX);
    }
    
    public PartitionIndex(
            File localPath, 
            ObsCache obsCache,
            TableName tableInfo,
            String initPartitionIndexFileName) {

        
        this.localPath =  localPath;
        this.obsCache = obsCache;
        this.tableInfo = tableInfo;
        if(initPartitionIndexFileName != null) {
            try {
                reload(this.tableInfo,initPartitionIndexFileName);
            }catch(Exception e) {
                throw new OrcaObjectStoreException(e);
            }
        }
    }
    
//    public PartitionIndex(
//            File localPath, 
//            ObsCache obsCache,
//            TableName tableInfo) {
//       this(localPath,  obsCache, tableInfo, null);
//    }
    
    public Long getNextIndexId() {
        
        if (patitionId == null) {
            patitionId = new AtomicLong(0);
        }
        Long nextId = patitionId.incrementAndGet();
        return nextId;
    }
    
    public void add(Partition index) throws Exception {
        synchronized (lock) {
            if (index.getId() == -1) {
                index.setId(getNextIndexId());
            }
            index.setCreateTimestamp(System.currentTimeMillis());
            this.partitions.put(index.getStartKey(),index);
        }
    }
    
    public Partition getPartitionIndexByRowkey( byte[] rowkey) throws Exception {
        synchronized (lock) {
            //查找在 [startkey ,endkey] 之间的数据
            Map.Entry<byte[], Partition> entry = this.partitions.floorEntry(rowkey);
            if(entry!=null) {
                Partition index = entry.getValue();
                if(Bytes.compareTo(rowkey,index.getEndKey()) <= 0 
                        && (index.getRowCount() > 0 || index.getDataSize() > 0 )) {
                    index.setLastAccessTimestamp(System.currentTimeMillis());
                    return index;
                }
                else {
                    if(_log.isTraceEnabled()) {
                        _log.trace("table:{} not searched partiton ,startKey:{},endKey:{},search key:{}",
                                index.getTableName(),
                                Bytes.toHex(index.getStartKey()),
                                Bytes.toHex(index.getEndKey()),
                                Bytes.toHex(rowkey));
                    }
                }
            }
            return null;
        }
   }
 
    public ConcurrentSkipListMap<byte[],Partition> getPartitionIndexByRowkeyRange(
            byte[] startRowKey, 
            byte[] endRowKey) throws Exception {
        synchronized (lock) {
            if (this.partitions == null) {
                return null;
            }
            if (startRowKey == null && endRowKey == null) {
                return this.partitions;
            }
            byte[] toKey = null;
            if(endRowKey != null) {
                toKey = this.partitions.ceilingKey(endRowKey);
            }
            ConcurrentNavigableMap<byte[],Partition> tmp = null;
            
            if(startRowKey == null && toKey != null) {
                tmp = this.partitions.headMap(toKey, false);
            }else if(startRowKey != null && toKey != null) {
                tmp = this.partitions.subMap(startRowKey, true, toKey, false);
            }
            else {
                tmp = this.partitions.tailMap(startRowKey, true);
            }
            if(tmp == null || tmp.size() == 0) {
                return null;
            }
            
            ConcurrentSkipListMap<byte[],Partition> indexs = new ConcurrentSkipListMap<>(
                    new Comparator<byte[]>(){
                        @Override
                        public int compare(byte[] o1, byte[] o2) {
                            return Bytes.compareTo(o1, o2);
                        }
                    } 
                );
            for(Map.Entry<byte[],Partition> data : tmp.entrySet()) {
                Partition index = data.getValue();
                index.setLastAccessTimestamp(System.currentTimeMillis());
                indexs.put(data.getKey(),index);
            }
            return indexs;
        }
    }

    public String getFileNameByRowkey(byte[] rowkey) throws Exception {
        synchronized (lock) {
            Map.Entry<byte[], Partition> entry = this.partitions.floorEntry(rowkey);
            if(entry!=null) {
                Partition index = entry.getValue();
                index.setLastAccessTimestamp(System.currentTimeMillis());
                return index.getDataFileName();
            }
            return null;
        }
    }

    public ObsFileReference flush() throws IOException {
        synchronized (lock) {
            long version = this.getNextIndexVersion();
            String indexFileName = getIndexFileName(version);
            
            File databasePathObj = new File(localPath, tableInfo.getDatabasePath());
            File outFile = new File(databasePathObj, indexFileName);
             
            String outFilePath = outFile.getAbsolutePath();
            try {
                if (this.partitions != null && this.partitions.size() > 0) {
                    PartitionIndexWarp warp = new PartitionIndexWarp();
                    List<Partition> partitionLists = new ArrayList<>();
                    this.partitions.forEach(new BiConsumer<byte[],Partition>() {
                        @Override
                        public void accept(byte[] t, Partition u) {
                            u.setRemoteTimestamp(UberTime.getTime());
                            partitionLists.add(u);
                        }
                    });
                    warp.setPartitions(partitionLists);
                    warp.setMaxId(patitionId);
                    File file = new File(outFilePath);
                    String data = UberUtil.toJson(warp);
                    FileUtils.writeStringToFile(file, data);
                   
                    String objectKey = tableInfo.getDatabasePath() + indexFileName;
    
                    ObsFileReference fileCache = new ObsFileReference(objectKey, file,"partition");
                    return fileCache;
                }
                return null;
            }
            catch (Exception e) {
                throw new OrcaObjectStoreException(e);
            }
        }
    }

    public String getIndexFileVersionPath() {
        long indexVersion = getCurrentIndexVersion();
        String versionPartitionFileName = tableInfo.getDatabasePath() + getIndexFileName( indexVersion);
        return versionPartitionFileName;
    }
   
    private void reload(TableName tableInfo,String partitionIndexFileName) throws Exception {
        
        PartitionIndexWarp datas = new PartitionIndexWarp();

        if (HdfsStorageService.TABLE_SYNC_PARAM.equals(tableInfo.getTableName())) {
            return;
        }

        String objectKey = tableInfo.getDatabasePath() + partitionIndexFileName;
        ObsFileReference file = obsCache.get(objectKey);
        if (file != null) {
            Pattern r = Pattern.compile(PARTITION_PATTERN);
            Matcher m = r.matcher(partitionIndexFileName);
            if (m.find( )) {
                String version = m.group(VERSION);
                _log.debug("paser table file name:{}, version:{}",partitionIndexFileName,version);
                int indexVersion = Bytes.toInt(Bytes.fromHex(version));
                patitionVersion = new AtomicLong(indexVersion);
            } 
            try {
                String contents = FileUtils.readFileToString(file.getFile().getAbsoluteFile());
                datas = UberUtil.toObject(contents, PartitionIndexWarp.class);
            }
            catch (Exception e) {
                throw new OrcaObjectStoreException(e);
            }
            if (datas != null && datas.getPartitions() != null && datas.getPartitions().size() > 0) {
                AtomicLong maxId = datas.getMaxId();
                this.patitionId = maxId;
                synchronized (lock) {
                    for(Partition partition:datas.getPartitions()) {
                        this.partitions.put(partition.getStartKey(), partition);
                    }
                }
            }
        }
    }
 
    private long getCurrentIndexVersion() {
        if (patitionVersion == null) {
            patitionVersion = new AtomicLong(0); 
        }
        return patitionVersion.get();
    }

    private long getNextIndexVersion() {
        if (patitionVersion == null) {
            patitionVersion = new AtomicLong(0);
        }
        Long nextId = patitionVersion.incrementAndGet();
        return nextId;
    }
    
    public void setPartitions(ConcurrentSkipListMap<byte[], Partition> partitions) {
        synchronized (lock) {
            this.partitions = partitions;
        }
    }
}