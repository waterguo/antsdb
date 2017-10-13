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
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.nosql.ConfigService;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.IndexLine;
import com.antsdb.saltedfish.nosql.Replicable;
import com.antsdb.saltedfish.nosql.ReplicationHandler;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.SpaceManager;
import com.antsdb.saltedfish.nosql.StorageEngine;
import com.antsdb.saltedfish.nosql.StorageTable;
import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.sql.OrcaConstant;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.util.LongLong;
import com.antsdb.saltedfish.util.UberTimer;
import com.antsdb.saltedfish.util.UberUtil;

public class HBaseStorageService implements StorageEngine, Replicable {
    static Logger _log = UberUtil.getThisLogger();
    
    Configuration hbaseConfig = null;			// hbase configuration
    Connection hbaseConnection = null;      	// hbase connection (thread-safe)
    User hbaseUser = null;
    Humpback humpback = null;					// humpback for handler to use
    MetadataService metaService = null;			// MetadataService to get Table Meta from ANTSDB
    CheckPoint cp;
 
    int bufferSize = 2000;						// size of sync buffer 
    int maxColumnPerPut = 2500;					// maximum column included in one put(rows)
    
    Algorithm compressionType = Algorithm.GZ;	// compression type: GZ by default (NONE, SNAPPY, LZO, LZ4)

	long startTrxId = 0;
	HBaseReplicationHandler replicationHandler;
    ConcurrentMap<Integer, HBaseTable> tableById = new ConcurrentHashMap<>();

    private boolean isMutable;

    public HBaseStorageService(Humpback humpback) throws Exception {
        this.humpback = humpback;
    }

    public void shutdown() throws IOException {
        if (this.hbaseConnection == null) {
            return;
        }
    	
    	// save current SP to hBase
        this.cp.updateHBase();
    	
        // HBase connection
    	this.hbaseConnection.close();
        this.hbaseConfig.clear();
        this.hbaseConnection = null;
        
		_log.debug("HBase disconnected.");
    }
    
    public boolean isConnected() {
    	return (this.hbaseConnection != null && !this.hbaseConnection.isClosed());
    }
     
    public long getCurrentSP() {
    	return this.cp.getCurrentSp();
    }
    
    public void updateLogPointer(long currentSP) throws IOException {
    	this.cp.updateLogPointer(currentSP);
    }

    public int getConfigBufferSize() {
    	return this.bufferSize;
    }
    
    @Override
    public void open(File home, ConfigService antsdbConfig, boolean isMutable) throws Exception {
        this.isMutable = isMutable;
        
        // options used by hbase service
        this.bufferSize = antsdbConfig.getHBaseBufferSize();
        this.maxColumnPerPut = antsdbConfig.getHBaseMaxColumnsPerPut();        
        String compressCodec = antsdbConfig.getHBaseCompressionCodec();
        this.compressionType = Algorithm.valueOf(compressCodec.toUpperCase());
        
        // Configuration object
        this.hbaseConfig = HBaseConfiguration.create();
        for (Map.Entry<Object, Object> i:antsdbConfig.getProperties().entrySet()) {
            String key = (String)i.getKey();
            if (key.startsWith("hbase.") || key.startsWith("zookeeper.")) {
                this.hbaseConfig.set(key, (String)i.getValue());
            }
        }
       
        try {
            // Connection is thread-safe and heavy weight object so we create one and share it
            if (User.isHBaseSecurityEnabled(this.hbaseConfig)) {
                
                System.setProperty("java.security.krb5.realm", "BLUE-ANTS.CLOUDAPP.NET");
                System.setProperty("java.security.krb5.kdc", "blue-ants.cloudapp.net");
    
                String hbasePrinc = this.hbaseConfig.get("hbase.master.kerberos.principal", "");
                String hbaseKeytabFile = hbaseConfig.get("hbase.master.keytab.file", "");
                
                UserGroupInformation.setConfiguration(this.hbaseConfig);            
                UserGroupInformation userGroupInformation = 
                        UserGroupInformation.loginUserFromKeytabAndReturnUGI(hbasePrinc, hbaseKeytabFile);
                UserGroupInformation.setLoginUser(userGroupInformation);
                hbaseUser = User.create(userGroupInformation);
                hbaseConnection = ConnectionFactory.createConnection(hbaseConfig, hbaseUser);
    
                /*
                hbaseUser.runAs(new PrivilegedExceptionAction<Object>() {
                    @Override
                    public Object run() throws Exception {
                        Connection hConnection = ConnectionFactory.createConnection(hbaseConfig);
                        // Create table
                        try (Admin admin = hConnection.getAdmin()) {
                            // Create namespace first
                            NamespaceDescriptor nsDescriptor = NamespaceDescriptor.create("TEST").build();
                            admin.createNamespace(nsDescriptor);
                            
                            HTableDescriptor table = new HTableDescriptor(TableName.valueOf("TEST", "Kerberos"));
                            table.addFamily(new HColumnDescriptor(Helper.SYS_COLUMN_FAMILY));
                            table.addFamily(new HColumnDescriptor(Helper.DATA_COLUMN_FAMILY));
                            admin.createTable(table);
                        } catch (Exception ex) {
                            throw new OrcaHBaseException("Failed to create table - TEST:kerberos", ex);
                        }
                        return hConnection;
                    }
                });
                */
            }
            else {
                this.hbaseConnection = ConnectionFactory.createConnection(hbaseConfig);
            }
        }
        catch (Throwable x) {
            throw x;        
        }
        
        try {
            _log.info("HBase is connected ");

            // Initialize HBase database for antsdb
            init();
        }
        catch (Throwable x) {
            this.hbaseConnection.close();
            throw x;
        }
    }

    private void init() throws Exception {
    	// create antsdb namespaces and tables if they are missing
    	
    	setup();
    	
    	// load checkpoint
    	
    	this.cp = new CheckPoint(humpback, this.hbaseConnection, this.isMutable);
    	
    	// load system tables
    	
    	Admin admin = this.hbaseConnection.getAdmin();
    	TableName[] tables = admin.listTableNamesByNamespace(OrcaConstant.SYSNS);
    	for (TableName i:tables) {
    	    String name = i.getQualifierAsString();
    	    if (!name.startsWith("x")) {
    	        continue;
    	    }
    	    int id = Integer.parseInt(name.substring(1), 16);
    	    SysMetaRow meta = new SysMetaRow(id);
    	    meta.setNamespace(i.getNamespaceAsString());
    	    meta.setTableName(name);
    	    meta.setType(TableType.DATA);
    	    HBaseTable table = new HBaseTable(this, meta);
    	    this.tableById.put(id, table);
    	}
    	
    	// validations
    	
    	if (this.cp.serverId != this.humpback.getServerId()) {
    		throw new OrcaHBaseException("hbase is currently linked to a different antsdb instance");
    	}
    	if (this.cp.getCurrentSp() > this.humpback.getSpaceManager().getAllocationPointer()) {
    		throw new OrcaHBaseException("hbase synchronization pointer is ahead of local log");
    	}
    	
    	// update checkpoint
    	
    	if (this.isMutable) {
    	    this.cp.updateHBase();
    	}
    }
    
    private void setup() throws OrcaHBaseException {
        if (!this.isMutable) {
            return;
        }
    	if (!Helper.existsNamespace(this.hbaseConnection, OrcaConstant.SYSNS)) {
    		_log.info("namespace {} is not found in HBase, creating ...", OrcaConstant.SYSNS);
    		createNamespace(OrcaConstant.SYSNS);
    	}
    	if (!Helper.existsTable(this.hbaseConnection, OrcaConstant.SYSNS, CheckPoint.TABLE_SYNC_PARAM)) {
    		_log.info("checkpoint table {} is not found in HBase, creating ...", CheckPoint.TABLE_SYNC_PARAM);
    		createTable(OrcaConstant.SYSNS, CheckPoint.TABLE_SYNC_PARAM);
    	}
    }

    @Override
    public synchronized void createNamespace(String namespace) throws OrcaHBaseException {
        if (!this.isMutable) {
            throw new OrcaHBaseException("hbase storage is in read-only mode");
        }
    	Helper.createNamespace(this.hbaseConnection, namespace);
    }

    @Override
    public synchronized void deleteNamespace(String namespace) throws OrcaHBaseException {        
        if (!this.isMutable) {
            throw new OrcaHBaseException("hbase storage is in read-only mode");
        }
    	Helper.dropNamespace(this.hbaseConnection, namespace);
    }
    
    @Override
    public synchronized  StorageTable createTable(SysMetaRow meta) throws OrcaHBaseException {
        if (!this.isMutable) {
            throw new OrcaHBaseException("hbase storage is in read-only mode");
        }
        String namespace = meta.getNamespace();
        String tableName = meta.getTableName();
    	Helper.createTable(this.hbaseConnection, namespace, tableName, this.compressionType);
    	HBaseTable table = new HBaseTable(this, meta);
    	this.tableById.put(meta.getTableId(), table);
    	return table;
    }

    void createTable(String namespace, String tableName) {
        if (!this.isMutable) {
            throw new OrcaHBaseException("hbase storage is in read-only mode");
        }
        Helper.createTable(this.hbaseConnection, namespace, tableName, this.compressionType);
    }
    
    public boolean existsTable(String namespace, String tableName) {
    	return Helper.existsTable(this.hbaseConnection, namespace, tableName);
    }
    
    @Override
    public synchronized boolean deleteTable(int tableId) {
        if (!this.isMutable) {
            throw new OrcaHBaseException("hbase storage is in read-only mode");
        }
        HBaseTable table = this.tableById.get(tableId);
        if (table == null) {
            throw new IllegalArgumentException();
        }
    	Helper.dropTable(this.hbaseConnection, table.meta.getNamespace(), table.meta.getTableName());
    	return true;
    }
        
	List<byte[]> tableColumnQualifierList = new ArrayList<byte[]>();
	byte[] tableColumnTypes = null;
	
	void updateColumnInfo(int tableId, Row row) {
		
		int maxColumnId = row.getMaxColumnId();
		
		if (this.tableColumnQualifierList.size() < maxColumnId + 1) {
			TableMeta tableMeta = getTableMeta(tableId);
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
	
	/**
	 * 
	 * @param tableId
	 * @return null if the table is deleted
	 */
	Mapping getMapping(int tableId) {
        SysMetaRow tableInfo = this.humpback.getTableInfo(tableId);
        if (tableInfo == null) {
            throw new OrcaHBaseException("humpback metadata for table {} is not found", tableId);
        }
        if (tableInfo.isDeleted()) {
            return null;
        }
        TableMeta tableMeta = getTableMeta(tableId);
        if ((tableId >= 0x100) && (tableMeta == null)) {
            throw new OrcaHBaseException("orca metadata for table {} is not found", tableId);
        }
        Mapping mapping = new Mapping(tableInfo, tableMeta);
        return mapping;
	}
	
	public void put1(int tableId, List<Row> rows) throws IOException  {    	
        if (!this.isMutable) {
            throw new OrcaHBaseException("hbase storage is in read-only mode");
        }
        
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
    		updateColumnInfo(tableId, row);
    		
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
    
	public synchronized void delete(int tableid, long pkey, long trxid, long sp) throws IOException {
        if (!this.isMutable) {
            throw new OrcaHBaseException("hbase storage is in read-only mode");
        }
		// Get table object

    	TableName tableName = getTableName(tableid);
        if (tableName == null) {
        	// table is deleted
            return;
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
    	SysMetaRow metarow = this.humpback.getTableInfo(tableId);
    	if (metarow == null) {
    	    throw new OrcaHBaseException("metadata of table {} is not found", tableId);
    	}
    	if (metarow.isDeleted()) {
    	    return null;
    	}
    	return TableName.valueOf(metarow.getNamespace(), metarow.getTableName());
	}

	public TableMeta getTableMeta(int tableId) {
		if (this.metaService == null) {
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
            TableMeta table = getTableMeta(tableId);
	    	return Helper.toRow(heap, r, table, tableId);
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

    @Override
    public StorageTable getTable(int tableId) {
        return this.tableById.get(tableId);
    }

    @Override
    public boolean isTransactionRecoveryRequired() {
        return true;
    }

    @Override
    public LongLong getLogSpan() {
        LongLong result = new LongLong(0, this.cp.getCurrentSp());
        return result;
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
    public void gc(long timestamp) {
        // nothing to gc
    }

    @Override
    public void close() throws IOException {
        shutdown();
    }

    @Override
    public boolean supportReplication() {
        return true;
    }

    @Override
    public long getReplicateLogPointer() {
        return this.cp.getCurrentSp();
    }

    @Override
    public ReplicationHandler getReplayHandler() {
        if (this.replicationHandler == null) {
            this.replicationHandler = new HBaseReplicationHandler(humpback, this);
        }
        return this.replicationHandler;
    }

    @Override
    public void syncTable(SysMetaRow meta) {
        if (this.tableById.get(meta.getTableId()) != null) {
            return;
        }
        HBaseTable table = new HBaseTable(this, meta);
        this.tableById.put(meta.getTableId(), table);
    }

    @Override
    public synchronized void deletes(int tableId, List<Long> keys) {
        if (!this.isMutable) {
            throw new OrcaHBaseException("hbase storage is in read-only mode");
        }
        try {
            this.tableById.get(tableId);
            TableName tableName = getTableName(tableId);
            
            // skip data from already dropped table     
            if (tableName == null) {
                return;
            }
            Table htable = this.hbaseConnection.getTable(tableName);
            List<Delete> deletes = new ArrayList<>();
            for (Long pkey:keys) {
                byte[] key = Helper.antsKeyToHBase(pkey);
                Delete delete = new Delete(key);
                deletes.add(delete);
           }
           htable.delete(deletes);
        }
        catch (IOException x) {
            throw new OrcaHBaseException(x);
        }
   }

    @Override
    public synchronized void putRows(int tableId, List<Long> rows) {
        if (!this.isMutable) {
            throw new OrcaHBaseException("hbase storage is in read-only mode");
        }
        try {
            SysMetaRow tableInfo = this.humpback.getTableInfo(tableId);
            if (tableInfo == null) {
                throw new OrcaHBaseException("metadata of table {} is not found", tableId);
            }
            if (tableInfo.isDeleted()) {
                return;
            }
            Mapping mapping = getMapping(tableId);
            List<Put> puts = new ArrayList<>();
            for (Long pRow:rows) {
                Row row = Row.fromMemoryPointer(pRow, 0);
                Put put = Helper.toPut(mapping, row);
                puts.add(put);
            }
            if (puts.size() > 0) {
                Table htable = this.hbaseConnection.getTable(mapping.getTableName());
                htable.put(puts);
            }
        }
        catch (IOException x) {
            throw new OrcaHBaseException(x);
        }
    }

    @Override
    public synchronized void putIndexLines(int tableId, List<Long> indexLines) {
        if (!this.isMutable) {
            throw new OrcaHBaseException("hbase storage is in read-only mode");
        }
        try {
            SysMetaRow tableInfo = this.humpback.getTableInfo(tableId);
            if (tableInfo == null) {
                throw new OrcaHBaseException("metadata of table {} is not found", tableId);
            }
            if (tableInfo.isDeleted()) {
                return;
            }
            TableName tn = getTableName(tableId);
            List<Put> puts = new ArrayList<>();
            for (Long pLine:indexLines) {
                IndexLine line = IndexLine.from(pLine);
                Put put = Helper.toPut(line);
                puts.add(put);
            }
            if (puts.size() > 0) {
                Table htable = this.hbaseConnection.getTable(tn);
                htable.put(puts);
            }
        }
        catch (IOException x) {
            throw new OrcaHBaseException(x);
        }
   }

    @Override
    public boolean exist(int tableId) {
        TableName tn = getTableName(tableId);
        return Helper.existsTable(this.hbaseConnection, tn);
    }
}