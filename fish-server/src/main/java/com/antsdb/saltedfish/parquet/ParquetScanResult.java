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

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.parquet.example.data.Group;
import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.FlexibleHeap;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.nosql.IndexLine;
import com.antsdb.saltedfish.nosql.InterruptException;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.ScanResult;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.obs.cache.ObsCache;
import com.antsdb.saltedfish.obs.cache.ObsFileReference;
import com.antsdb.saltedfish.parquet.bean.Partition;
import com.antsdb.saltedfish.parquet.merge.MergerUtils;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author Frank Li<lizc@tg-hd.com>
 */
public class ParquetScanResult extends ScanResult {
    
    private static Logger _log = UberUtil.getThisLogger();
    private static byte[] _start = {0x0};
    
    private long pRow;
    @SuppressWarnings("unused")
    private int rowScanned;
    private Heap heap;
    private boolean eof = false;
    private TableMeta meta;
    private int tableId;
    private long pKey;
    private long pIndexRowKey;
    private byte misc;
    private TableType type;
    private ParquetDataReader reader = null; 
    private ConcurrentSkipListMap<byte[],Partition> partitions = null;
    private Group cache;
    private boolean endFlagNumber = true;
    private TableName tableInfo;
    private Scan scan;
    private String filePathName;
     
    private  ObsCache obsCache;
    
    private byte[] startKey;
    
    public ParquetScanResult(
            TableMeta meta, 
            ConcurrentSkipListMap<byte[],Partition> partitions, 
            int tableId,
            TableType type, 
            TableName tableInfo,
            Scan scan, 
            ObsCache obsCache
            )throws Exception {
      
        this.meta = meta;
        this.heap = new FlexibleHeap();
        this.tableId = tableId;
        this.type = type;
        this.tableInfo = tableInfo;
        
        this.scan = scan;
        //this.startKey = scan.getStartRow();
        this.partitions = partitions;
        this.obsCache = obsCache;
    }
 
    private boolean hasNextDataFile() {
        boolean hasNextData = false;
        if(this.partitions == null 
                || this.partitions.isEmpty() 
                || this.partitions.size() == 0) {
            return false;
        }
        if(startKey == null && this.partitions.size() > 0) {
            if(scan.getStartRow() == null || Bytes.equals(_start,scan.getStartRow())) {
                hasNextData = true;
            }
            else {
                hasNextData = firstCheck();
            }
        }
        else if(scan.getStopRow() !=null && Bytes.compareTo(startKey, scan.getStopRow()) > 0){
            hasNextData = false; 
        }else {
            // 返回与严格大于给定键的最小键关联的键-值映射关系；如果不存在这样的键，则返回 null。
            for(;;) {
                Map.Entry<byte[],Partition> temp = this.partitions.higherEntry(startKey);
                if(temp != null) {
                    Partition tmpPartition = temp.getValue();
                    if(tmpPartition.getRowCount() > 0 
                            && tmpPartition.getDataSize() > 0
                            && (Bytes.compareTo(startKey, tmpPartition.getStartKey()) >=0 
                                 ||Bytes.compareTo(startKey, tmpPartition.getEndKey()) <=0) 
                            ) {
                        hasNextData = true;
                        break;
                    }
                    else {
                        startKey = tmpPartition.getEndKey();
                    }
                }
                else {
                    break;
                }
            }
        }
        return hasNextData;
    }
    
    /**
     * First, find the start value less than or equal to;
     * Second, find a value greater than or equal to.
     * @return
     */
    private boolean firstCheck() {
      //返回与小于等于给定键的最大键关联的键-值映射关系；如果不存在这样的键，则返回 null。
        Map.Entry<byte[],Partition> temp =  this.partitions.floorEntry(scan.getStartRow());
        if(temp != null) {
            Partition tmpPartition = temp.getValue();
            if(tmpPartition.getRowCount() > 0 
                    && tmpPartition.getDataSize() > 0
                    && Bytes.compareTo(scan.getStartRow(), tmpPartition.getStartKey()) >=0 
                    && Bytes.compareTo(scan.getStartRow(), tmpPartition.getEndKey()) <=0 
                    ) {
                return true;
            }
        }
        else {
            temp =  this.partitions.higherEntry(scan.getStartRow());
            if(temp != null) {
                Partition tmpPartition = temp.getValue();
                if(tmpPartition.getRowCount() > 0 
                   && tmpPartition.getDataSize() > 0 
                   && Bytes.compareTo(scan.getStartRow(), tmpPartition.getStartKey()) >=0 
                   && Bytes.compareTo(scan.getStartRow(), tmpPartition.getEndKey()) <=0 
                   ) {
                    return true;
                }
            }
        }
        return false;
    }

    private Partition getDatafileIndex() {
        if (hasNextDataFile()) {
            Partition partition = null;
            Map.Entry<byte[],Partition> temp = null;
            for(;;) {
                if(startKey == null) {
                    if(scan.getStartRow() == null || Bytes.equals(_start,scan.getStartRow())) {
                        temp =  this.partitions.firstEntry();
                    }
                    else {
                        //返回与小于等于给定键的最大键关联的键-值映射关系；如果不存在这样的键，则返回 null。
                        temp =  this.partitions.floorEntry(scan.getStartRow());
                        if(temp == null) {
                            temp =this.partitions.higherEntry(scan.getStartRow());
                        }
                    }
                }
                else {
                    temp =this.partitions.higherEntry(startKey);
                }
                if(temp!=null) {
                    Partition tmpPartition = temp.getValue();
                    if(tmpPartition.getRowCount() > 0 
                            && tmpPartition.getDataSize() > 0
                            && (startKey == null 
                                    || Bytes.compareTo(startKey, tmpPartition.getStartKey()) >=0 
                                    ||Bytes.compareTo(startKey, tmpPartition.getEndKey()) <=0) 
                            ) {
                        partition =  tmpPartition;
                        startKey = partition.getEndKey();
                        break;
                    }
                    else {
                        startKey = tmpPartition.getEndKey();
                    }
                }
                else {
                    break;
                }
            }
            return partition;
        }
        else {
            endFlagNumber = true;
            return null;
        }
    }

    private void readerClose() throws Exception {
        if (reader != null) {
            reader.close();// read next file
            reader = null;
        }
    }

    private boolean checkRange(byte[] rowKey) {
        if (scan == null) {
            return true;
        }
        if (scan.getStartRow() == null && scan.getStopRow() == null){
            return true;
        }
        if(scan.getStartRow() == null) {
            return checkEnd(rowKey);
        }
        if(scan.getStopRow() == null) {
            return checkStart(rowKey);
        }
        boolean resultStart = checkStart(rowKey);
        
        if(!resultStart) {
            return false;
        }
        
        boolean resultEnd = checkEnd(rowKey);
        
        if(resultEnd) {
            return true;
        }
        return false;
    }
    
    private boolean checkStart(byte[] rowKey) {
        boolean resultStart = false;
        if(scan.isIncStart()) {
            resultStart = Bytes.compareTo(rowKey, scan.getStartRow()) >= 0;
        }
        else {
            resultStart = Bytes.compareTo(rowKey, scan.getStartRow()) > 0;
        }
        return resultStart;
    }
    
    private boolean checkEnd(byte[] rowKey) {
        boolean resultEnd = false;
        if(scan.isIncEnd()) {
            resultEnd = Bytes.compareTo(rowKey, scan.getStopRow()) <= 0;
        }
        else {
            resultEnd = Bytes.compareTo(rowKey, scan.getStopRow()) < 0;
        }
        return resultEnd;
    }

    private void readData() throws Exception { 
        if (reader == null || reader.isClosed()) {
            if(hasNextDataFile()) {
                initRead(2);
            }
            else {
                endFlagNumber = true;
                return;
            }
        }
        if (reader == null || reader.isClosed()) {
            endFlagNumber = true;
            return;
        }
        while ((cache = reader.readNext()) != null) {
            byte[] rowKey = MergerUtils.getKeyByGroup(cache, type);
            boolean range = checkRange(rowKey);
            if(_log.isTraceEnabled()){
                _log.trace("read data by table={} tableId={} type={} file={} rowkey/key={} range={},scan startkey={},endkey={}", 
                    tableInfo.getTableName() ,
                    tableId,
                    this.type,
                    filePathName,
                    Bytes.toHex(rowKey),
                    range, 
                    scan.getStartRow() !=null ? Bytes.toHex(scan.getStartRow()):"null", 
                    scan.getStopRow() !=null ? Bytes.toHex(scan.getStopRow()):"null"
                    );
            }
            if (range) {
                endFlagNumber = false;
                return ;
            }
        }
         
        readerClose();
        if (hasNextDataFile()) {
            readData();
        }
        else {
            endFlagNumber = true;
            return;
        }
    }

    @Override
    public boolean next() {
        if (Thread.interrupted()) {
            throw new InterruptException();
        }
        try {
            if (partitions == null && this.partitions.size() == 0) {
                this.pRow = 0;
                this.eof = true;
                return false;
            }
            readData();
            if(_log.isTraceEnabled()) {
                _log.trace("read data over table={} tableId={} type={} endFlagNumber:{} scan startkey:{},endkey:{}", 
                        tableInfo.getTableName() ,
                        tableId,
                        this.type,
                        endFlagNumber,
                        scan.getStartRow() !=null ? Bytes.toHex(scan.getStartRow()):"null", 
                        scan.getStopRow() !=null ? Bytes.toHex(scan.getStopRow()):"null"
                        );
            }
            if (cache == null || endFlagNumber) {
                this.pRow = 0;
                this.eof = true;
                return false;
            }
            if(_log.isTraceEnabled()) {
                _log.trace("have data table={} tableId={} type={} endFlagNumber:{} scan startkey:{},endkey:{}", 
                        tableInfo.getTableName() ,
                        tableId,
                        this.type,
                        endFlagNumber, 
                        scan.getStartRow() !=null ? Bytes.toHex(scan.getStartRow()):"null", 
                        scan.getStopRow() !=null ? Bytes.toHex(scan.getStopRow()):"null"
                        );
            }
            this.heap.reset(0);
            if (this.type == TableType.DATA) {
                this.pRow = Helper.toRow(heap, cache, meta, this.tableId);
                this.pKey = Row.getKeyAddress(this.pRow);
            }
            else {
                long tmpMisc = Helper.toIndexLine(heap, cache);
                if(_log.isTraceEnabled()) {
                    _log.trace("have index data table={} tableId={} tmpMisc={}", 
                            tableInfo.getTableName() ,
                            tableId,
                            tmpMisc
                            );
                }
                if (tmpMisc >= 0) {
                    IndexLine indexLine = new IndexLine(tmpMisc);
                    this.pKey = indexLine.getKey();
                    this.pIndexRowKey = indexLine.getRowKey();
                    this.misc = indexLine.getMisc();
                }
            }
            this.rowScanned++;
            return true;
        }
        catch (Exception x) {
            _log.error("{}\tscan data error:{}" ,tableInfo.getTableName(),x.getMessage());
            throw new OrcaObjectStoreException(x,"{}\tscan data error:{}" ,tableInfo.getTableName(),x.getMessage());
        }
    }

    @Override
    public boolean eof() {
        return this.eof;
    }

    @Override
    public void close() {
        try {
            readerClose();
        }
        catch (Exception e) {
            _log.error(tableInfo.getTableName() + "\tscan close error:" + e.getMessage(), e);
        }
        if (this.heap != null) {
            this.heap.free();
            this.heap = null;
        }
    }

    @Override
    public long getVersion() {
        if (this.pRow == 0) {
            return 0;
        }
        long version = Row.getVersion(this.pRow);
        return version;
    }

    @Override
    public long getKeyPointer() {
        return this.pKey;
    }

    @Override
    public long getIndexRowKeyPointer() {
        return this.pIndexRowKey;
    }

    @Override
    public long getRowPointer() {
        return this.pRow;
    }

    @Override
    public byte getMisc() {
        return this.misc;
    }

    @Override
    public void rewind() {}

    @Override
    public String toString() {
        return getLocation();
    }
    
    @Override
    public String getLocation() {
        return "parquet:" + KeyBytes.toString(getKeyPointer());
    }
    
    private void initRead(int call) throws Exception {
        Partition fileIndex = getDatafileIndex();
        if (fileIndex != null) {
            readerClose();
            _log.info("table={} call={} type={} get datafile index id:{},verion:{}", 
                    this.tableInfo.getTableName(),
                    call,
                    this.type,
                    fileIndex.getId(),
                    fileIndex.getVersion());
            filePathName = tableInfo.getTablePath() + fileIndex.getVersionFileName();
           
            String obejctKey =  tableInfo.getTablePath() + fileIndex.getVersionFileName();
            ObsFileReference obsFile = obsCache.get(obejctKey);
            endFlagNumber = false;
            long fileSize = 0;
            if(obsFile != null) {
                fileSize = obsFile.getFsize();
                if (fileSize > ParquetDataWriter.minParquetSize) {
                    Path path = new Path(obsFile.getFile().getAbsolutePath());
                    _log.info("table={} type={} scan load data file={} fsize={}",
                            this.tableInfo.getTableName(),
                            this.type,
                            obsFile.getFile().getAbsolutePath(),
                            fileSize);
                    try {
                        reader = new ParquetDataReader(path, new Configuration());
                    }
                    catch (Exception e) {
                        _log.error(e.getMessage(), e);
                    }
                }
                else {
                    _log.warn("table={} file error={} size={}",
                            this.tableInfo.getTableName(),
                            obsFile.getFile().getAbsolutePath(), 
                            fileSize);
                    obsCache.remove(obejctKey);
                    initRead(3);
                }
            }
            else {
                endFlagNumber = true;
            }
        }
        else {
            readerClose();
            endFlagNumber = true;
        }
    }
}
