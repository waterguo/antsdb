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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.AllocPoint;
import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.cpp.FileOffset;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.cpp.MemoryManager;
import com.antsdb.saltedfish.cpp.OutOfHeapException;
import com.antsdb.saltedfish.nosql.GetInfo;
import com.antsdb.saltedfish.nosql.IndexLine;
import com.antsdb.saltedfish.nosql.InterruptException;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.ScanOptions;
import com.antsdb.saltedfish.nosql.ScanResult;
import com.antsdb.saltedfish.nosql.StorageTable;
import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.obs.ObsProvider;
import com.antsdb.saltedfish.obs.cache.ObsCache;
import com.antsdb.saltedfish.parquet.bean.Partition;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.KeyMaker;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author Frank Li<lizc@tg-hd.com>
 */
public class ParquetTable implements StorageTable  {
    private static Logger _log = UberUtil.getThisLogger();
    
    protected static ThreadLocal<BluntHeap> _heap = ThreadLocal.withInitial(() -> {
        ByteBuffer buf = MemoryManager.allocImmortal(AllocPoint.HBASE_READ_BUFFER, 64 * 1024);
        return new BluntHeap(buf);
    });

    protected ObsService storageService;
    protected TableName tn;
    protected int tableId;
    protected SysMetaRow rowMeta;
    protected boolean isBlobTable;
    protected ObsProvider client;
    protected ObsCache obsCache;
    
    private boolean isDelete;
    private  MessageType schema;
    private PartitionIndex partitionIndex;
    
    private File localDataDirectory;
    
    @Override
    public PartitionIndex getPartitionIndex() {
        return partitionIndex;
    }
    
    @Override
    public void setPartitionIndex(PartitionIndex partitionIndex) {
        this.partitionIndex = partitionIndex;
    }

    @Override
    public boolean isDelete() {
        return isDelete;
    }

    public void setDelete(boolean isDelete) {
        this.isDelete = isDelete;
    }

    @Override
    public long get(long pKey, long option, GetInfo info) {
        if (Thread.interrupted()) {
            throw new InterruptException();
        }
        try {
            if (_log.isTraceEnabled()) {
                _log.trace("table={} get pKey={} option={}", 
                        this.tn.getTableName(), 
                        KeyBytes.toString(pKey),
                        option
                        );
            }
            
            byte[] getKey = Helper.antsKeyToHdfs(pKey);
            
            StorageTable table = this.storageService.getTable(tn.getTableId());
            PartitionIndex partitionIndex = table.getPartitionIndex();
            if(partitionIndex == null) {
                partitionIndex = new PartitionIndex(
                        this.localDataDirectory,
                        this.obsCache,
                        this.tn,
                        this.storageService.getInitPartitiFile(tn.getTableId()));
                table.setPartitionIndex(partitionIndex);
            }
            Partition partition = partitionIndex.getPartitionIndexByRowkey(getKey);
            if(partition == null) {
                return 0;
            }
            Result r = Helper.get(partition,this.obsCache,tn, getKey);
            if (r != null) {
                if (r.isEmpty()) {
                    return 0;
                }
                int size = Helper.getSize(r);
                long pRow = Helper.toRow(getHeap(size * 2 + 0x100), r.getGroup(), getTableMeta(), this.tableId);
                return pRow;
            }
            else {
                _log.info("dbname={} tbname={} tableId={} not found by {}!",
                        tn.getDatabaseName(),
                        tn.getTableName(),
                        this.tableId,
                        pKey);
                return 0;
            }
        }
        catch (OutOfHeapException x) {
            _log.error("error table={} key={}", 
                    this.tableId, 
                    KeyBytes.toString(pKey));
            throw x;
        }
        catch (Exception x) {
            _log.error(x.getMessage(),x);
            throw new OrcaObjectStoreException(x,
                    "dbname={} tbname={} tableId={} errorMessage:{}",
                    tn.getDatabaseName(),
                    tn.getTableName(),
                    this.tableId,
                    x.getMessage());
        }
    }

    @Override
    public long getIndex(long pKey, long options, GetInfo info) {
        if (_log.isTraceEnabled()) {
            _log.trace("getIndex {} {}", this.tn.toString(), KeyBytes.toString(pKey));
        }
        if (Thread.interrupted()) {
            throw new InterruptException();
        }
        try {
            byte[] getKey = Helper.antsKeyToHdfs(pKey);
            StorageTable table = this.storageService.getTable(tn.getTableId());
            PartitionIndex partitionIndex =table.getPartitionIndex();
            if(partitionIndex == null) {
                partitionIndex = new PartitionIndex(
                        this.localDataDirectory,
                        this.obsCache,
                        this.tn,
                        this.storageService.getInitPartitiFile(tn.getTableId()));
                table.setPartitionIndex(partitionIndex);
            }
            Partition partition = partitionIndex.getPartitionIndexByRowkey(getKey);
            Result r = Helper.getIndex(partition,this.obsCache,tn, getKey);
            if (r==null || r.isEmpty()) {
                return 0;
            }
            int size = Helper.getSize(r);
            long pLine = Helper.toIndexLine(getHeap(size * 2 + 0x100), r.getGroup());
            if (pLine <= 0) {
                return 0;
            }
            IndexLine line = new IndexLine(pLine);
            return line.getRowKey();
        }
        catch (Exception x) {
            throw new OrcaObjectStoreException(x);
        }
    }

    @Override
    public String toString() {
        return this.tn.toString();
    }

    private Heap getHeap(int size) {
        BluntHeap result = _heap.get();
        if (result.capacity() < size) {
            ByteBuffer buf = (result != null) ? result.getBuffer() : null;
            buf = MemoryManager.growImmortal(AllocPoint.HBASE_READ_BUFFER, buf, size);
            result = new BluntHeap(buf);
            _heap.set(result);
        }
        result.reset(0);
        return result;
    }

    @Override
    public void delete(long pKey) {
        throw new NotImplementedException();
    }

    @Override
    public void putIndex(long pIndexKey, long pRowKey, byte misc) {
        throw new NotImplementedException();
    }

    @Override
    public void putIndex(long version, long pIndexKey, long pRowKey, byte misc) {
        throw new NotImplementedException();
    }

    @Override
    public void put(Row row) {
        throw new NotImplementedException();
    }

    @Override
    public boolean exist(long pKey) {
        return get(pKey, 0, null) != 0;
    }

    @Override
    public String getLocation(long pKey) {
        return "hdfs:" + KeyBytes.toString(pKey);
    }

    @Override
    public boolean traceIo(long pKey, List<FileOffset> lines) {
        return false;
    }

    protected TableMeta getTableMeta() {
        TableMeta meta = this.storageService.getTableMeta(this.isBlobTable ? this.tableId - 1 : this.tableId);
        return meta;
    }

    public static void freeMemory() {
        BluntHeap heap = _heap.get();
        if (heap != null) {
            MemoryManager.freeImmortal(AllocPoint.HBASE_READ_BUFFER, heap.getBuffer());
            _heap.remove();
        }
    }

    public String getNamespace() {
        return this.getRowMeta().getNamespace();
    }

    public String getTableName() {
        return this.getRowMeta().getTableName();
    }
    
    @Override
    public SysMetaRow getRowMeta() {
        return rowMeta;
    }
    
    @Override
    public void setSchema(MessageType schema) {
        this.schema = schema;
    }
    
    public MessageType getSchema() {
        return schema;
    }
   
    public ParquetTable(
            ObsService storageService, 
            SysMetaRow meta, 
            ObsProvider client,
            ObsCache obsCache,
            File localDataDirectory) {
        this.rowMeta = meta;
        this.tableId = meta.getTableId();
        this.storageService = storageService;
        this.isBlobTable = meta.getTableName().endsWith("_blob_");
        String ns = meta.getNamespace();
        ns = (ns.equals(Orca.SYSNS)) ? storageService.getSystemNamespace() : ns;
        this.tn = TableName.valueOf(ns, meta.getTableName(), this.tableId);
        this.client = client;
        this.obsCache = obsCache;
        this.localDataDirectory = localDataDirectory;
    }

    @Override
    public ScanResult scan(long pKeyStart, long pKeyEnd, long options) {
        boolean incStart = ScanOptions.includeStart(options);//是否包含起始值
        boolean incEnd = ScanOptions.includeEnd(options);
        boolean isAscending = ScanOptions.isAscending(options);

        try {
            Scan scan = new Scan();
            scan.setIncStart(incStart);
            scan.setIncEnd(incEnd);
            byte[] start = null;
            if (pKeyStart != 0) {
                start = KeyBytes.get(pKeyStart);
                KeyMaker.flipEndian(start);
                scan.setStartRow(start);
            }
            if (pKeyEnd != 0) {
                byte[] end = KeyBytes.get(pKeyEnd);
                KeyMaker.flipEndian(end);
                scan.setStopRow(end);
            }
            
            if(_log.isTraceEnabled()) {
                _log.trace("scan table={} pKeyStart={} incStart={} startKey={} pKeyEnd={} incEnd={} endKey={} options={}", 
                        this.tn.getTableNameAndId(), 
                        KeyBytes.toString(pKeyStart),
                        incStart ? "true" : "ˉ",
                        scan.getStartRow()!=null?Bytes.toHex(scan.getStartRow()):"null",
                        KeyBytes.toString(pKeyEnd), 
                        incEnd ? "true" : "ˉ",
                        scan.getStopRow()!=null?Bytes.toHex(scan.getStopRow()):"null",
                        options);
            }
            
            scan.setReversed(!isAscending);
            if(this.getPartitionIndex() == null) {
                partitionIndex = new PartitionIndex(
                        this.localDataDirectory,
                        this.obsCache,
                        this.tn,
                        this.storageService.getInitPartitiFile(tn.getTableId()));
            }
            if(this.getPartitionIndex() != null && this.getPartitionIndex().getPartitions() != null ) {
                ConcurrentSkipListMap<byte[],Partition> partitionByTable = this.getPartitionIndex().getPartitions();
                
                ScanResult result = null ;
                if(isAscending) {
                    result = new ParquetScanResult(
                            getTableMeta(), 
                            partitionByTable,
                            this.tableId,
                            this.getRowMeta().getType(),
                            tn, 
                            scan,
                            obsCache);
                }
                else {
                    result = new ParquetScanResultDesc(
                            getTableMeta(), 
                            partitionByTable,
                            this.tableId,
                            this.getRowMeta().getType(),
                            tn, 
                            scan,
                            obsCache);
                }
                return result;
            }
            else {
                throw new OrcaObjectStoreException("table {} partitionIndex or partitions is null",this.tn);
            }
        }
        catch (Exception x) {
            throw new OrcaObjectStoreException(x);
        }
    }
    
    @Override
    public ConcurrentSkipListMap<byte[],Partition> getPartitions(){
       if(this.partitionIndex!=null) {
           return this.partitionIndex.getPartitions();
       }
       return null;
    }
}
