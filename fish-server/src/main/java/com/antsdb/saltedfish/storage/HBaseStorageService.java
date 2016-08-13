/*-------------------------------------------------------------------------------------------------
 _______ __   _ _______ _______ ______  ______
 |_____| | \  |    |    |______ |     \ |_____]
 |     | |  \_|    |    ______| |_____/ |_____]

 Copyright (c) 2016, antsdb.com and/or its affiliates. All rights reserved. *-xguo0<@

 This program is free software: you can redistribute it and/or modify it under the terms of the
 GNU Affero General Public License, version 3, as published by the Free Software Foundation.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/agpl-3.0.txt>
-------------------------------------------------------------------------------------------------*/
package com.antsdb.saltedfish.storage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.nosql.ConfigService;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.SpaceManager;
import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.sql.OrcaConstant;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.util.UberTimer;
import com.antsdb.saltedfish.util.UberUtil;

public class HBaseStorageService {
    static Logger _log = UberUtil.getThisLogger();
    
    File hbaseConfHome = null;					// hbase configuration home
    Configuration hbaseConfig = null;			// hbase configuration
    Connection hbaseConnection = null;      	// hbase connection (thread-safe)
    HBaseStorageServerThread hbaseSyncThread;	// background thread to sync data to HBase
    Humpback humpback = null;					// humpback for handler to use
    MetadataService metaService = null;			// MetadataService to get Table Meta from ANTSDB
    CheckPoint cp;
 
    int bufferSize = 2000;						// size of sync buffer 
    int maxColumnPerPut = 2500;					// maximum column included in one put(rows)
    
    Algorithm compressionType = Algorithm.GZ;	// compression type: GZ by default (NONE, SNAPPY, LZO, LZ4)

	long startTrxId = 0;

    public HBaseStorageService(ConfigService antsdbConfig, Humpback humpback) throws Exception {
    	
    	// options used by hbase service
        this.bufferSize = antsdbConfig.getHBaseBufferSize();
        this.maxColumnPerPut = antsdbConfig.getHBaseMaxColumnsPerPut();        
        String compressCodec = antsdbConfig.getHBaseCompressionCodec();
    	this.compressionType = Algorithm.valueOf(compressCodec.toUpperCase());
        
    	// Check HBase conf folder for configuration
    	File hbaseConfFile = antsdbConfig.getHBaseConfFile();
    	if (!hbaseConfFile.isFile()) {
            throw new OrcaHBaseException("HBase config file is not found - {}", hbaseConfFile); 
        }
    	_log.info("found HBase config file: {}", hbaseConfFile.getAbsolutePath());
        
        // Configuration object
        this.hbaseConfig = HBaseConfiguration.create();
        this.hbaseConfig.addResource(new Path(hbaseConfFile.getAbsolutePath()));
       
        // Connection is thread-safe and heavy weight object so we create one and share it
        this.hbaseConnection = ConnectionFactory.createConnection(hbaseConfig);
		try {
	        this.humpback = humpback;

			_log.info("HBase is connected ");

			// Initialize HBase database for antsdb
			init();
			
	        // Start background HBaseServer thread
	        this.hbaseSyncThread = new HBaseStorageServerThread(this, this.humpback);
		}
		catch (Throwable x) {
			this.hbaseConnection.close();
			throw x;
		}
    }

    public void shutdown() throws IOException {
    	
    	// background thread
        this.hbaseSyncThread.shutdown();
        
    	// save current SP to hBase
        this.cp.setOpen(false);
        this.cp.updateHBase();
    	
        // HBase connection
    	this.hbaseConnection.close();
        this.hbaseConfig.clear();
        this.hbaseConnection = null;
        
		_log.debug("HBase disconnected.");
    }
    
    public void setPaused(boolean paused) {
    	this.hbaseSyncThread.setPaused(paused);
    }
    
    public boolean isConnected() {
    	return (this.hbaseConnection != null && !this.hbaseConnection.isClosed());
    }
     
    public long getCurrentSP() {
    	return this.cp.getCurrentSp();
    }
    
    public void setCurrentSP(long currentSP) {
    	this.cp.setCurrentSp(currentSP);
    }

    public int getConfigBufferSize() {
    	return this.bufferSize;
    }
    
    private void init() throws Exception {
    	// create antsdb namespaces and tables if they are missing
    	
    	setup();
    	
    	// load checkpoint
    	
    	this.cp = new CheckPoint(humpback, this.hbaseConnection);
    	
    	// validations
    	
    	if (this.cp.isOpen) {
    		// hbase is already opened by another antsdb or it crashed last time
    		_log.warn("hbase wasn't closed properly by antsdb last time");
    	}
    	if (this.cp.serverId != this.humpback.getServerId()) {
    		throw new OrcaHBaseException("hbase is currently linked to a different antsdb instance");
    	}
    	if (this.cp.getCurrentSp() > this.humpback.getSpaceManager().getAllocationPointer()) {
    		throw new OrcaHBaseException("hbase synchronization pointer is ahead of local log");
    	}
    	
    	// update checkpoint
    	
    	this.cp.setOpen(true);
    	this.cp.updateHBase();
    }
    
    private void setup() throws OrcaHBaseException {
    	if (!Helper.existsNamespace(this.hbaseConnection, OrcaConstant.SYSNS)) {
    		_log.info("namespace {} is not found in HBase, creating ...", OrcaConstant.SYSNS);
    		createNamespace(OrcaConstant.SYSNS);
    	}
    	if (!Helper.existsTable(this.hbaseConnection, OrcaConstant.SYSNS, CheckPoint.TABLE_SYNC_PARAM)) {
    		_log.info("checkpoint table {} is not found in HBase, creating ...", CheckPoint.TABLE_SYNC_PARAM);
    		createTable(OrcaConstant.SYSNS, CheckPoint.TABLE_SYNC_PARAM);
    	}
    }
    
    public synchronized void createNamespace(String namespace) throws OrcaHBaseException {        
    	Helper.createNamespace(this.hbaseConnection, namespace);
    }

    public synchronized void dropNamespace(String namespace) throws OrcaHBaseException {        
    	Helper.dropNamespace(this.hbaseConnection, namespace);
    }
    
    public synchronized void createTable(String namespace, String tableName) throws OrcaHBaseException {        
    	Helper.createTable(this.hbaseConnection, namespace, tableName, this.compressionType);
    }
   
    public boolean existsTable(String namespace, String tableName) {
    	return Helper.existsTable(this.hbaseConnection, namespace, tableName);
    }
    
    public synchronized void dropTable(String namespace, String tableName) {
    	Helper.dropTable(this.hbaseConnection, namespace, tableName);
    }
        
    public synchronized void truncateTable(int tableid, long sp) throws OrcaHBaseException {
    	TableName tableName = getTableName(tableid);
    	if (tableName != null) {
    		try {
            	// truncate table from hbase
        		Helper.truncateTable(this.hbaseConnection, 
        				tableName.getNamespaceAsString(), tableName.getQualifierAsString());

        		// save truncate table sp list
        		this.cp.setTruncateTableSp(tableid, sp);
    		}
    		catch (Exception ex) {
                throw new OrcaHBaseException("Failed to truncate table - " + tableName, ex);
    		}
        }
    	else {
    		throw new OrcaHBaseException("Truncate Table not found - " + tableid);
    	}
    }
    
	List<byte[]> tableColumnQualifierList = new ArrayList<byte[]>();
	byte[] tableColumnTypes = null;
	void updateColumnInfo(Row row) {
		
		int id = row.getTableId();
		int maxColumnId = row.getMaxColumnId();
		
		if (this.tableColumnQualifierList.size() < maxColumnId + 1) {
			TableMeta tableMeta = getTable(id);
	    	TableName tableName = getTableName(id);
	    	
	    	int oldMaxColumnId = this.tableColumnQualifierList.size() - 1;
	    	if (oldMaxColumnId > 0 &&  oldMaxColumnId < maxColumnId) {
	    		_log.info("Table {}  - max column id changed from {} to {}",
	    						tableName.toString(), oldMaxColumnId, maxColumnId);
	    	}
	    	
			this.tableColumnQualifierList.clear();
			this.tableColumnTypes = new byte[maxColumnId+1];

	        for (int i=0; i<=maxColumnId; i++) {
	        	long pValue = row.getFieldAddress(i); 
	        	this.tableColumnTypes[i] = Helper.getType(pValue);
	        	
	    		byte[] qualifier = getColumnName(tableMeta, i);
	    		this.tableColumnQualifierList.add(qualifier);
	        }
		}
 	}
	
	public synchronized void put(List<HBaseStorageSyncBuffer.RowData> rows) throws IOException  {    	
    	int tableId = rows.get(0).getRow().getTableId();
    	TableName tableName = getTableName(tableId);
    	
    	// skip data from already dropped table    	
    	if (tableName == null) {
    		return;
    	}

    	this.tableColumnQualifierList.clear();
    	this.tableColumnTypes = null;

    	Table htable = this.hbaseConnection.getTable(tableName);
    	int totalColumns = 0;
    	ArrayList<Put> puts = new ArrayList<Put>(100);
    	
    	Row row;
    	for (HBaseStorageSyncBuffer.RowData rowData : rows) {
 
    		// If table already truncated we'll skip this row
    		if (this.cp.isTruncatedData(tableId, rowData.getSp())) {
    			continue;
    		}

    		// get Row
    		row = rowData.getRow();
    		
    		// update column info
    		updateColumnInfo(row);
    		
	        // populate row key    	
			byte[] key = Helper.antsKeyToHBase(row.getKeyAddress());
	    	Put put = new Put(key);
	
	    	// populate version
	    	
	    	// long version = this.humpback.getTrxMan().getTimestamp(Row.getVersion(row.getAddress()));
	    	long version = this.humpback.getTrxMan().getTimestamp(row.getVersion());
	    	if (version < 0) {
	    		throw new OrcaHBaseException("invalid version {}", version);
	    	}
			put.addColumn(Helper.SYS_COLUMN_FAMILY_BYTES, Helper.SYS_COLUMN_VERSION_BYTES, version, Bytes.toBytes(version));
	    	
			// populate size
			
			put.addColumn(Helper.SYS_COLUMN_FAMILY_BYTES, Helper.SYS_COLUMN_SIZE_BYTES, version, Bytes.toBytes(row.getLength()));
			
	    	// populate fields
			
			int maxColumnId = row.getMaxColumnId();
			byte[] types = new byte[maxColumnId+1];
	        for (int i=0; i<=maxColumnId; i++) {
				byte[] qualifier = tableColumnQualifierList.get(i);
	    		if (qualifier == null) {
	    			continue;
	    		}
	        	long pValue = row.getFieldAddress(i); 
				types[i] = Helper.getType(pValue);
	    		byte[] value = Helper.toBytes(pValue);
	    		put.addColumn(Helper.DATA_COLUMN_FAMILY_BYTES, qualifier, version, value);
	        }
	
	        // populate data types
			put.addColumn(Helper.SYS_COLUMN_FAMILY_BYTES, Helper.SYS_COLUMN_DATATYPE_BYTES, version, types);
			puts.add(put);
			totalColumns += put.size();
			
			// if total columns exceeds define maxColumnCount, we'll do one put
			if (totalColumns >= this.maxColumnPerPut) {
				htable.put(puts);
				puts.clear();
				totalColumns = 0;
			}
    	}

    	// do last put
		if (puts.size() > 0) {
    		htable.put(puts);
    		puts.clear();
    		totalColumns = 0;
    	}
		htable.close();
    }
    
	public void put1(List<Row> rows) throws IOException  {    	
    	int tableId = rows.get(0).getTableId();
    	TableName tableName = getTableName(tableId);
    	
    	// skip data from already dropped table    	
    	if (tableName == null) {
    		return;
    	}

    	this.tableColumnQualifierList.clear();
    	this.tableColumnTypes = null;

    	Table htable = this.hbaseConnection.getTable(tableName);
    	int totalColumns = 0;
    	ArrayList<Put> puts = new ArrayList<Put>(100);
    	
    	for (Row row : rows) {

    		// update column info
    		updateColumnInfo(row);
    		
	        // populate row key    	
			byte[] key = Helper.antsKeyToHBase(row.getKeyAddress());
	    	Put put = new Put(key);
	
	    	// populate version
	    	
	    	// long version = this.humpback.getTrxMan().getTimestamp(Row.getVersion(row.getAddress()));
	    	long version = this.humpback.getTrxMan().getTimestamp(row.getVersion());
	    	if (version < 0) {
	    		throw new OrcaHBaseException("invalid version {}", version);
	    	}
			put.addColumn(Helper.SYS_COLUMN_FAMILY_BYTES, Helper.SYS_COLUMN_VERSION_BYTES, version, Bytes.toBytes(version));
	    	
			// populate size
			
			put.addColumn(Helper.SYS_COLUMN_FAMILY_BYTES, Helper.SYS_COLUMN_SIZE_BYTES, version, Bytes.toBytes(row.getLength()));
			
	    	// populate fields
			
			int maxColumnId = row.getMaxColumnId();
			byte[] types = new byte[maxColumnId+1];
	        for (int i=0; i<=maxColumnId; i++) {
				byte[] qualifier = tableColumnQualifierList.get(i);
	    		if (qualifier == null) {
	    			continue;
	    		}
	        	long pValue = row.getFieldAddress(i); 
				types[i] = Helper.getType(pValue);
	    		byte[] value = Helper.toBytes(pValue);
	    		put.addColumn(Helper.DATA_COLUMN_FAMILY_BYTES, qualifier, version, value);
	        }
	
	        // populate data types
			put.addColumn(Helper.SYS_COLUMN_FAMILY_BYTES, Helper.SYS_COLUMN_DATATYPE_BYTES, version, types);
			puts.add(put);
			totalColumns += put.size();
			
			// if total columns exceeds define maxColumnCount, we'll do one put
			if (totalColumns >= this.maxColumnPerPut) {
				htable.put(puts);
				puts.clear();
				totalColumns = 0;
			}
    	}

    	// do last put
		if (puts.size() > 0) {
    		htable.put(puts);
    		puts.clear();
    		totalColumns = 0;
    	}
		htable.close();
    }
    
	public synchronized void index(List<HBaseStorageSyncBuffer.IndexData> indics) throws IOException {
		
       	int tableId = indics.get(0).getTableId();
    	TableName tableName = getTableName(tableId);
    	
    	// skip data from already dropped table    	
    	if (tableName == null) {
    		return;
    	}

    	Table htable = this.hbaseConnection.getTable(tableName);
    	int totalColumns = 0;
    	ArrayList<Put> puts = new ArrayList<Put>(100);
    	
    	long version;
    	for (HBaseStorageSyncBuffer.IndexData index : indics) {

    		// If table already truncated we'll skip this row
    		if (this.cp.isTruncatedData(tableId, index.getSp())) {
    			continue;
    		}
    		
 			// convert antsdb's key to normal key value (HBase key is user-readable)
			
			byte[] rowkey = Helper.antsKeyToHBase(index.getRowKey());
			byte[] indexKey = Helper.antsKeyToHBase(index.getIndexKey());

			// Initiate put and delete object
	    	Put put = new Put(indexKey);

	    	// get version of the row
	    	version = index.getTrxTs();

	    	// version
			put.addColumn(Helper.SYS_COLUMN_FAMILY_BYTES, Helper.SYS_COLUMN_VERSION_BYTES, version, Bytes.toBytes(version));
	    	// index key
			put.addColumn(Helper.SYS_COLUMN_FAMILY_BYTES, Helper.SYS_COLUMN_INDEXKEY_BYTES, version, rowkey);
	    	
			puts.add(put);
			totalColumns += put.size();
			
			// if total columns exceeds define maxColumnCount, we'll do one put
			if (totalColumns >= this.maxColumnPerPut) {
				htable.put(puts);
				puts.clear();
				totalColumns = 0;
			}
        }
    	
		// do last put
 		if (puts.size() > 0) {
    		htable.put(puts);
    		puts.clear();
    		totalColumns = 0;
    	}
		
 		htable.close();        	
	}

	public synchronized void delete(int tableid, long pkey, long trxid, long sp) throws IOException {

		// If table already truncated we'll skip this delete
		if (this.cp.isTruncatedData(tableid, sp)) return;
		
		// Get table object

    	TableName tableName = getTableName(tableid);
        if (tableName == null) {
        	// table is deleted
        }
        
        // Generate delete data
        
        long version = this.humpback.getTrxMan().getTimestamp(trxid);
        if (version < 0) {
        	throw new OrcaHBaseException("invalid version {}", version);
        }
    	byte[] key = Helper.antsKeyToHBase(pkey);
        Delete delete = new Delete(key, version); //3037956051773948088L); //Long.MAX_VALUE);
        
        // Delete row
        
        Table table = this.hbaseConnection.getTable(tableName);
		table.delete(delete);
    }
    
    public TableName getTableName(int tableId) {
    	if (tableId < 0) {
        	String name = String.format("%08x", tableId);
    		return TableName.valueOf(OrcaConstant.SYSNS, name);
    	}
    	SysMetaRow metarow = this.humpback.getTableInfo(tableId);
    	return (metarow != null) ? TableName.valueOf(metarow.getNamespace(), metarow.getTableName()) : null;
	}

	public TableMeta getTable(int tableId) {
		if (this.metaService == null) {
			return null;
		}
		if (tableId < 0) {
			return null;
		}
    	return this.metaService.getTable(Transaction.getSeeEverythingTrx(), tableId);
	}

	static byte[] getColumnName(TableMeta table, int columnId) {
		// columnId=0 means rowid, a system column
		
		if ((table != null) && (columnId > 0)) {
			ColumnMeta column = table.getColumnByColumnId(columnId);
			// column could be null if it is deleted
			if (column != null) {
				return Bytes.toBytes(column.getColumnName());
			}
		}
		
		// system table or rowid column
		
		byte[] qualifier = new byte[2];
		qualifier[0] = (byte)(columnId >> 8);
		qualifier[1] = (byte)columnId;
		return qualifier;
	}

	public boolean exists(int tableId, long pKey) {
    	TableName tableName = getTableName(tableId);
        if (tableName == null) {
        	throw new OrcaHBaseException("table id {} is invalid", tableId);
        }
        try {
    		byte[] key = Helper.antsKeyToHBase(pKey);
			Result r = Helper.exist(this.hbaseConnection, tableName, key);
	    	return !r.isEmpty();
		}
		catch (IOException x) {
			throw new OrcaHBaseException(x);
		}
	}

    public long get(Heap heap, int tableId, long trxid, long trxts, long pKey) {
    	TableName tableName = getTableName(tableId);
        if (tableName == null) {
        	throw new OrcaHBaseException("table id {} is invalid", tableId);
        }
        try {
    		byte[] key = Helper.antsKeyToHBase(pKey);
			Result r = Helper.get(this.hbaseConnection, tableName, key);
            TableMeta table = getTable(tableId);
	    	return Helper.toRow(heap, r, table);
		}
		catch (IOException x) {
			throw new OrcaHBaseException(x);
		}
    }
    
    public Map<String, byte[]> get_(String ns, String tn, byte[] key) 
	throws IOException {
    	TableName tableName = TableName.valueOf(ns, tn);
		Result r = Helper.get(this.hbaseConnection, tableName, key);
		if (r.isEmpty()) {
			return null;
		}
		Map<String, byte[]> row = new HashMap<>();
		for (Map.Entry<byte[],NavigableMap<byte[],byte[]>> i:r.getNoVersionMap().entrySet()) {
			String cf =  new String(i.getKey());
			for (Map.Entry<byte[],byte[]> j:i.getValue().entrySet()) {
				String q = new String(j.getKey());
				row.put(cf + ":" + q, j.getValue());
			}
		}
		return row;
    }
	
	public void setMetaService(MetadataService metaService) {
		this.metaService = metaService;
	}

	public HBaseScanResult scan(
			int tableid, 
			long pFrom, 
			boolean fromInclusive, 
			long pTo, 
			boolean toInclusive, 
			boolean isAscending) {
		try {
/*			Scan scan = new Scan();
			TableName tableName = getTableName(tableid);
			Table htable = this.hbaseConnection.getTable(tableName);
			if (htable == null) {
				throw new OrcaHBaseException("hbase table {} not found", tableName);
			}
			ResultScanner result= htable.getScanner(scan);
			TableMeta table = getTable(tableid);
			HBaseScanResult scanResult = new HBaseScanResult(result, table);
			return scanResult;
*/
			return null;
		}
		catch (Exception x) {
			throw new OrcaHBaseException(x);
		}
	}

	public HBaseScanResult scan(
			String ns,
			String name, 
			long pFrom, 
			boolean fromInclusive, 
			long pTo, 
			boolean toInclusive, 
			boolean isAscending) {
		try {
/*			Scan scan = new Scan();
			TableName tableName = TableName.valueOf(ns, name);
			Table htable = this.hbaseConnection.getTable(tableName);
			if (htable == null) {
				throw new OrcaHBaseException("hbase table {} not found", tableName);
			}
			ResultScanner result= htable.getScanner(scan);
			HBaseScanResult scanResult = new HBaseScanResult(result, null);
			return scanResult;
*/
			return null;
		}
		catch (Exception x) {
			throw new OrcaHBaseException(x);
		}
	}

	public List<Map<String,byte[]>> getAll(String ns, String name) {
		try {
			Scan scan = new Scan();
			TableName tableName = TableName.valueOf(ns, name);
			Table htable = this.hbaseConnection.getTable(tableName);
			if (htable == null) {
				throw new OrcaHBaseException("hbase table {} not found", tableName);
			}
			ResultScanner rs= htable.getScanner(scan);
			List<Map<String, byte[]>> list = new ArrayList<>();
			for (Result r=rs.next(); r != null; r=rs.next()) {
				Map<String,byte[]> row = Helper.toMap(r);
				list.add(row);
			}
			return list;
		}
		catch (Exception x) {
			throw new OrcaHBaseException(x);
		}
	}
    
	/**
	 * start the replication thread
	 */
	public void start() {
        this.hbaseSyncThread.start();
	}

	public boolean doesTableExists(String ns, String table) {
    	return Helper.existsTable(this.hbaseConnection, ns, table);
	}

	public void waitForSync(int timeoutSeconds) throws TimeoutException {
		SpaceManager spaceman = this.humpback.getSpaceManager();
		
		// find out the current space pointer
		
		long spNow = spaceman.getAllocationPointer();
		
		// write a bogus rollback so that spNow can be replayed
		
		this.humpback.getGobbler().logMessage("nothing");
		
		// wait until timeout 
		
		UberTimer timer = new UberTimer(timeoutSeconds * 1000);
		for (;;) {
			if (getCurrentSP() >= spNow) {
				break;
			}
			if (timer.isExpired()) {
				throw new TimeoutException();
			}
			try {
				Thread.sleep(100);
			}
			catch (InterruptedException ignored) {
			}
		}
	}
	
	public long getStartTrxId() {
		return this.startTrxId;
	}
}