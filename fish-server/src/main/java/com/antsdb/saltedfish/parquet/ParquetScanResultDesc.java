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
import java.util.Stack;
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
public class ParquetScanResultDesc extends ScanResult {
    
    private static Logger _log = UberUtil.getThisLogger();
    private static byte[] _end = {(byte) 0xFF,0x00};
    
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
    
    private ConcurrentSkipListMap<byte[],Partition> partitions = null;
     
    private boolean endFlagNumber = true;
    private TableName tableInfo;
    private Scan scan;
     
    private  ObsCache obsCache;
    
    private byte[] endKey;
    
    private Stack<Group> stack;
    private Group cache;
    
    public ParquetScanResultDesc(
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
        if(scan.isReversed()) {
            hasNextData = descHasNextDataFile();
        }
        else {
            hasNextData = false;
        }
        return hasNextData;
    }
    
    private boolean descHasNextDataFile() {
        boolean hasNextData = false;
        if(endKey == null && this.partitions.size() > 0) {
            if(scan.getStopRow() == null || Bytes.equals(_end,scan.getStopRow())) {
                hasNextData = true;
            }
            else {
                hasNextData = descFirstCheck();
            }
        }
        else if(scan.getStopRow() !=null && Bytes.compareTo(endKey, scan.getStopRow()) > 0){
            hasNextData = false; 
        }else {
            for(;;) {
                Map.Entry<byte[],Partition> temp = this.partitions.lowerEntry(endKey);
                if(temp != null) {
                    Partition tmpPartition = temp.getValue();
                    if(tmpPartition.getRowCount() > 0 && tmpPartition.getDataSize() > 0){
                        if (Bytes.compareTo(endKey, tmpPartition.getStartKey()) >=0
                                && Bytes.compareTo(endKey, tmpPartition.getEndKey()) <=0
                                ){
                                hasNextData = true;
                                break;
                        }
                        else if (Bytes.compareTo(endKey, tmpPartition.getEndKey()) >=0
                                && Bytes.compareTo(endKey, tmpPartition.getEndKey()) >=0
                                && Bytes.compareTo(scan.getStartRow(), tmpPartition.getEndKey()) <=0
                                ){
                                hasNextData = true;
                                break;
                        }
                        else if (Bytes.compareTo(scan.getStartRow(), tmpPartition.getEndKey()) >0){
                            break;
                        }
                        else  {
                            endKey = tmpPartition.getStartKey();
                        }
                    }
                    else {
                        endKey = tmpPartition.getStartKey();
                    }
                }
                else {
                    break;
                }
            }
        }
        return hasNextData;
    }
    
    private boolean descFirstCheck() {
          Map.Entry<byte[],Partition> temp =  this.partitions.higherEntry(scan.getStopRow());
          if(temp != null) {
              Partition tmpPartition = temp.getValue();
              if(tmpPartition.getRowCount() > 0 
                      && tmpPartition.getDataSize() > 0
                      && Bytes.compareTo(scan.getStopRow(), tmpPartition.getStartKey()) >=0 
                      ) {
                  return true;
              }
          }
          
          temp =  this.partitions.floorEntry(scan.getStopRow());
          if(temp != null) {
              Partition tmpPartition = temp.getValue();
              if(tmpPartition.getRowCount() > 0 
                 && tmpPartition.getDataSize() > 0 
                 && Bytes.compareTo(scan.getStopRow(), tmpPartition.getStartKey()) >=0 
                 ) {
                  return true;
              }
          }
          return false;
      }

    private Partition getDatafileIndex() {
        if (hasNextDataFile()) {
            Partition partition = null;
            Map.Entry<byte[],Partition> temp = null;
            for(;;) {
                if(endKey == null) {
                    if(scan.getStopRow() == null || Bytes.equals(_end,scan.getStopRow())) {
                        temp =  this.partitions.lastEntry();
                    }
                    else {
                        //返回与小于等于给定键的最大键关联的键-值映射关系；如果不存在这样的键，则返回 null。
                        temp =  this.partitions.higherEntry(scan.getStopRow());
                        if(temp == null) {
                            temp =this.partitions.floorEntry(scan.getStopRow());
                        }
                    }
                }
                else {
                    temp =this.partitions.lowerEntry(endKey);
                }
                if(temp!=null) {
                    Partition tmpPartition = temp.getValue();
                    if(tmpPartition.getRowCount() > 0 && tmpPartition.getDataSize() > 0) {
                        if(endKey == null) {
                            if (Bytes.compareTo(scan.getStopRow(), tmpPartition.getStartKey()) >=0
                                && Bytes.compareTo(scan.getStopRow(), tmpPartition.getEndKey()) <=0){
                                partition =  tmpPartition;
                                endKey = partition.getStartKey();
                                break;
                            }
                            else  {
                                endKey = tmpPartition.getStartKey();
                            }
                        }
                        else {
                            if (Bytes.compareTo(endKey, tmpPartition.getStartKey()) >=0
                                    && Bytes.compareTo(endKey, tmpPartition.getEndKey()) <=0
                                    ){
                                partition =  tmpPartition;
                                endKey = partition.getStartKey();
                                break;
                            }
                            else if (Bytes.compareTo(endKey, tmpPartition.getEndKey()) >=0
                                    && Bytes.compareTo(endKey, tmpPartition.getEndKey()) >=0
                                    && Bytes.compareTo(scan.getStartRow(), tmpPartition.getEndKey()) <=0
                                    ){
                                partition =  tmpPartition;
                                endKey = partition.getStartKey();
                                break;
                            }
                            else if (Bytes.compareTo(scan.getStartRow(), tmpPartition.getEndKey()) >0){
                                break;
                            }
                            else  {
                                endKey = tmpPartition.getStartKey();
                            }
                        }
                    }
                    else {
                        endKey = tmpPartition.getStartKey();
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
        if (this.stack == null || this.stack.isEmpty()) {
            if(hasNextDataFile()) {
                initRead(2);
                if ((this.stack == null || this.stack.isEmpty()) 
                        && hasNextDataFile()) {
                    initRead(4);
                }
            }
            else {
                endFlagNumber = true;
                return;
            }
        }
        if (this.stack == null || this.stack.isEmpty()) {
            endFlagNumber = true;
            return;
        }
        cache = this.stack.pop();
        
        endFlagNumber = false;
        return ;
            
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
            this.stack.clear();
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
    
    private void initStack() {
        if(this.stack == null) {
            this.stack = new Stack<>();
        }
        else {
            this.stack.clear();
        }
    }
    
    private void initRead(int call) throws Exception {
        initStack();
        Partition fileIndex = getDatafileIndex();
        if (fileIndex != null) { 
            _log.info("table={} call={} type={} get datafile index id:{},verion:{}", 
                    this.tableInfo.getTableName(),
                    call,
                    this.type,
                    fileIndex.getId(),
                    fileIndex.getVersion());
           
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
                    try ( ParquetDataReader reader = new ParquetDataReader(path, new Configuration())){
                        if(reader != null) {
                            Group temp = null;
                            while ((temp = reader.readNext()) != null) {
                                byte[] rowKey = MergerUtils.getKeyByGroup(temp, type);
                                boolean range = checkRange(rowKey);
                                if (range) {
                                    this.stack.push(temp);
                                }
                            }
                        }
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
            endFlagNumber = true;
        }
    }
}
