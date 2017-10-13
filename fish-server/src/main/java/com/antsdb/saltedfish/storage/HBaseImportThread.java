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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.sql.OrcaConstant;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.storage.HBaseUtilImporter.IndexDataRow;
import com.antsdb.saltedfish.storage.HBaseUtilImporter.TableRows;
import com.antsdb.saltedfish.util.UberUtil;

public class HBaseImportThread  extends Thread {
    static Logger _log = UberUtil.getThisLogger();
	
	Connection hbaseConn = null;
	Humpback humpback = null;
	MetadataService metaService = null;
	
	boolean waitingForFinish = false;
	boolean shuttingDown = false;
	boolean threadTerminated = false;
	boolean ignoreError = false;
	int columnsPerPut = 2500;
	
	static final int BUFFER_SIZE = 10;
	List<TableRows> buffer = Collections.synchronizedList(new ArrayList<TableRows>(BUFFER_SIZE));
   
	HBaseImportThread(Connection hbaseConn, Humpback humpback,
							MetadataService metaService, boolean ignoreError, int columnsPerPut) {
		this.hbaseConn = hbaseConn;
		this.humpback = humpback;
		this.metaService = metaService;
		this.ignoreError = ignoreError;
		this.columnsPerPut = columnsPerPut;
		
		this.waitingForFinish = false;
		this.shuttingDown = false;
		this.threadTerminated = false;
	}
	
	public boolean putBuffer(TableRows r) {
		if (!threadTerminated && buffer.size() < BUFFER_SIZE) {
			synchronized (buffer) {
				buffer.add(r);
			}			
			return true;
		}
	
		return false;
	}
	
	private TableRows getNext() {
		
		synchronized (buffer) {
			if (buffer.size() > 0) {
				TableRows r = buffer.get(0);
				buffer.remove(0);
				return r;
			}
		}
		
		return null;
	}
	
    @Override
    public void run() {
    	
   		_log.info("{} started...", getName());
   	 
   		try {
            mainloop();    
        }
        catch (Exception x) {
            _log.error("Hbase data importing thread failed to put some rows", x);
        }

		threadTerminated = true;
		
		_log.info("{} exit.", getName());		
    }
    
	private void mainloop() throws Exception {
		
    	for (;;) {
    		
    		if (shuttingDown) {
    			break;
    		}
    		
    		TableRows r = getNext();
			if (r != null) {

				// put into hbase
				try {
					
					put(r);
					
				}
				catch (Exception e) {
					
        			_log.error("{} Failed to put rows to hbase table [{}]", getName(), this.tableName.toString(), e);
        			System.out.println("Failed to put rows to hbase table - " + this.tableName.toString());
        			e.printStackTrace();
        			
        			if (!ignoreError) {
        				throw e;
        			}
				}
			}
			else {
				
				// no rows waiting, exit if waiting for finish
				if (waitingForFinish) {
					break;
				}
				
				try {
					Thread.sleep(100);
				}
				catch(Exception e) {
				}
			}
		}
	}

	public void waitingForFinish() {
		waitingForFinish = true;
	}
	
	public void shutdown() {
		shuttingDown = true;
	}
	
	public boolean isTerminated() {
		return !this.isAlive() || threadTerminated;
	}
    
	
	// cached table information - so we don't have to get it for each batch of rows
	// we'll update it when table changed or maxColumnId changed
	
	int tableId = Humpback.SYSMETA_TABLE_ID;
	TableName tableName;
	List<byte[]> tableColumnQualifierList = new ArrayList<byte[]>();
	byte[] tableColumnTypes;
	Table htable = null;
	
	void updateTableInfo(int id) throws IOException {
		
		if (this.tableId != id) {
		
	    	this.tableName = getTableName(id);
	        this.tableId = id;
	        
	        if (this.htable != null) {
	        	this.htable.close();
	        	this.htable = null;
	        }
	        this.htable = this.hbaseConn.getTable(this.tableName);

	        // table changed, we need update column info!
	        this.tableColumnQualifierList.clear();
		}
	}
	
	void updateColumnInfo(int tableId, Row row) {
		
		int maxColumnId = row.getMaxColumnId();
		
		if (this.tableColumnQualifierList.size() < maxColumnId + 1) {
			
			TableMeta tableMeta = getTable(tableId);
	    	
	    	int oldMaxColumnId = this.tableColumnQualifierList.size() - 1;
	    	if (this.tableId == tableId && oldMaxColumnId > 0 &&  oldMaxColumnId < maxColumnId) {
	    		_log.info("Table {}  - max column id changed from {} to {}",
	    						this.tableName.toString(), oldMaxColumnId, maxColumnId);
	    	}
	    	
			this.tableColumnTypes = new byte[maxColumnId+1];
			this.tableColumnQualifierList.clear();
			
	        for (int i=0; i<=maxColumnId; i++) {
	        	long pValue = row.getFieldAddress(i); 
	        	this.tableColumnTypes[i] = Helper.getType(pValue);
	        	
	    		byte[] qualifier = getColumnName(tableMeta, i);
	    		this.tableColumnQualifierList.add(qualifier);
	        }
		}
 	}
	
	byte[] getColumnName(TableMeta table, int columnId) {
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
	
    TableName getTableName(int tableId) {
    	if (tableId < 0) {
        	String name = String.format("%08x", tableId);
    		return TableName.valueOf(OrcaConstant.SYSNS, name);
    	}
    	SysMetaRow metarow = this.humpback.getTableInfo(tableId);
    	return (metarow != null) ? TableName.valueOf(metarow.getNamespace(), metarow.getTableName()) : null;
	}

	TableMeta getTable(int tableId) {
		if (this.metaService == null) {
			return null;
		}
		if (tableId < 0) {
			return null;
		}
    	return this.metaService.getTable(Transaction.getSeeEverythingTrx(), tableId);
	}
	
	void put(TableRows r) throws Exception {
		if (r.isIndex()) {
			putIndex(r.getTableId(), r.getIndexRows());
		}
		else {
			put(r.getTableId(), r.getRows());
		}
	}
	
	void putIndex(int tableId, List<IndexDataRow> rows) throws IOException {

    	// update table info
		updateTableInfo(tableId);
   	
    	// skip data from already dropped table    	
    	
    	if (this.tableName == null) {
    		return;
    	}
    	
    	ArrayList<Put> puts = new ArrayList<Put>(rows.size());
    	for (IndexDataRow row : rows) {
    		
	        // populate row key    	

			byte[] indexKey = Helper.antsKeyToHBase(row.getIndexKeyPointer());

			// populate row key
	    	byte[] rowKey = Helper.antsKeyToHBase(row.getRowKeyPointer());
	    	
	    	// populate version
	    	long version = row.getVersion();
	    	if (version < 0) {
	    		throw new OrcaHBaseException("invalid version {}", version);
	    	}

			// Initiate put and delete object
	    	Put put = new Put(indexKey);

	    	// version
			put.addColumn(Helper.SYS_COLUMN_FAMILY_BYTES, Helper.SYS_COLUMN_VERSION_BYTES, version, Bytes.toBytes(version));
			
	    	// index key
			put.addColumn(Helper.SYS_COLUMN_FAMILY_BYTES, Helper.SYS_COLUMN_INDEXKEY_BYTES, version, rowKey);

			puts.add(put);
			
			// put into htable if total columns exceeding predefined
			if (puts.size() * 2 >= this.columnsPerPut) {
				htable.put(puts);
				puts.clear();
			}
    	}

    	// put last package into hbase
    	if (puts.size() > 0) {
    		htable.put(puts);		
    	}
	}
	
	void put(int tableId, List<Row> rows) throws IOException  {
    	// update table info
    	updateTableInfo(tableId);
    	
    	// skip data from already dropped table    	
    	if (this.tableName == null) {
    		return;
    	}
    	
    	ArrayList<Put> puts = new ArrayList<Put>(rows.size());
    	int totalColumns = 0;
    	for (Row row : rows) {
    		
	        // populate row key    	
    		
			byte[] key = Helper.antsKeyToHBase(row.getKeyAddress());
	    	Put put = new Put(key);
	
	    	// populate version
	    	
	    	long version = row.getVersion();
	    	if (version < 0) {
	    		throw new OrcaHBaseException("invalid version {}", version);
	    	}
			put.addColumn(Helper.SYS_COLUMN_FAMILY_BYTES, Helper.SYS_COLUMN_VERSION_BYTES, 
							version, Bytes.toBytes(version));
	    	
			// populate size
			
			put.addColumn(Helper.SYS_COLUMN_FAMILY_BYTES, Helper.SYS_COLUMN_SIZE_BYTES, 
							version, Bytes.toBytes(row.getLength()));
			
	    	// populate fields this is necessary because maxColumnId is not always the same for all rows in a table
			
			updateColumnInfo(tableId, row);	
			
			int maxColumnId = row.getMaxColumnId();
			for (int i=0; i<=maxColumnId; i++) {
				byte[] qualifier = tableColumnQualifierList.get(i);
	    		if (qualifier == null) {
	    			continue;
	    		}
	        	long pValue = row.getFieldAddress(i); 
	    		byte[] value = Helper.toBytes(pValue);
	    		put.addColumn(Helper.DATA_COLUMN_FAMILY_BYTES, qualifier, version, value);
	        }
	
	        // populate data types
			put.addColumn(Helper.SYS_COLUMN_FAMILY_BYTES, Helper.SYS_COLUMN_DATATYPE_BYTES, 
							version, this.tableColumnTypes);
    		
			puts.add(put);

			totalColumns += put.size();

			// put into hbase if total columns exceeding predefined value
			if (totalColumns >= this.columnsPerPut) {
				htable.put(puts);
				puts.clear();
				totalColumns = 0;
			}
    	}

    	// put last package into hbase
    	if (puts.size() > 0) {
    		htable.put(puts);
    	}
    }
}
