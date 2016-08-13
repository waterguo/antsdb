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
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaConstant;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.Transaction;

public class HBaseUtilImporterSingleThread {

	static final int ROWS_PER_PUT = 100;
	
	Orca orca = null;
	Connection hbaseConn = null;
	Humpback humpback = null;
	MetadataService metaService = null;
	Algorithm compressionType = Algorithm.GZ;
	
	public HBaseUtilImporterSingleThread(Orca orca, Connection hbaseConn) {
		this.orca = orca;
		this.hbaseConn = hbaseConn;
		this.humpback = orca.getHumpback();
		this.metaService = orca.getMetaService();
	}
	
	public void run() throws IOException {
		long importStart = System.currentTimeMillis(); 
		
		// init hbase
		System.out.println("*********** Check and initialize hbase DB for antsdb ***********");
		initHBaseDB(this.hbaseConn);

		long start, end, count;
		List<Row> rows = null;
		List<String> namespaces = humpback.getNamespaces();
		
		rows =  new ArrayList<Row>(100);
		for (String ns : namespaces) {
			System.out.println("*********** Namespace [" + ns + "] ***********");
			
			// get table list from antsdb
			Collection<GTable> tables = humpback.getTables(ns);
			TableName tableName;
			
			for (GTable table : tables) {			
				
				tableName = getTableName(table.getId());
				System.out.println("==== Table " + tableName.getQualifierAsString() + " ====");
				
				// create hbase table
				Helper.createTable(this.hbaseConn, tableName.getNamespaceAsString(), 
							tableName.getQualifierAsString(), this.compressionType);
				System.out.println("  Table created ok");
				
				// import table rows
				System.out.println("  Importing rows starts...");
				start = System.currentTimeMillis();
				count = 0;
				
				RowIterator scanner = table.scan(0, Long.MAX_VALUE);
				while (scanner.next()) {
					Row row = scanner.getRow();
					rows.add(row);
					
					if (rows.size() >= ROWS_PER_PUT) {
						put(rows);						
						count += rows.size();
						if ((count % 20000) == 0) {
							System.out.println("  Done rows: " + count);
						}
						
						rows.clear();
					}
				}

				if (rows.size() > 0) {
					put(rows);
					count += rows.size();
					System.out.println("  Done rows: " + count);
					rows.clear();;
				}
				
				end = System.currentTimeMillis();
				System.out.println("==== Table " + tableName.toString() + " done. Total rows: " + 
								count + ", " + (end - start) + " ms\n");
			}
		}
		// write current SP to __SYS.SYNCPARAM
		long currentSP = getCurrentSP();
		System.out.println("==== Update SYNCPARAM to " + currentSP + "====\n");
    	CheckPoint.updateHBase(this.hbaseConn, currentSP, false, this.humpback.getServerId(), null);
    	
		long importEnd = System.currentTimeMillis(); 
		
		System.out.println("\n\nHBase import finished. Total time: " + (importEnd - importStart) / 1000 + " seconds");
	}
	
	private long getCurrentSP() {
		long end = 0;
    	for (GTable i:this.humpback.getTables()) {
    		long sp = i.getEndRowSpacePointer();
    		end = Math.max(end, sp);
    	}
    	return end;
    }
	
	private void initHBaseDB(Connection conn) throws OrcaHBaseException {
		
    	if (!Helper.existsNamespace(conn, OrcaConstant.SYSNS)) {
    		System.out.println("Namespace " + OrcaConstant.SYSNS + " is not found in HBase, creating ...");
    		Helper.createNamespace(conn, OrcaConstant.SYSNS);
    	}
    	if (!Helper.existsTable(conn, OrcaConstant.SYSNS, CheckPoint.TABLE_SYNC_PARAM)) {
    		System.out.println("Checkpoint table " + CheckPoint.TABLE_SYNC_PARAM + 
    					" is not found in HBase, creating ...");
    		Helper.createTable(conn, OrcaConstant.SYSNS, CheckPoint.TABLE_SYNC_PARAM, this.compressionType);
    	}
    }
    
	int tableId = Integer.MAX_VALUE;
	TableName tableName;
	List<byte[]> tableColumnQualifierList = new ArrayList<byte[]>();
	byte[] tableColumnTypes;
	
	void getTableInfo(Row row) {
		int id = row.getTableId();
		if (this.tableId != id) {			
			TableMeta tableMeta = getTable(id);
	    	this.tableName = getTableName(id);
	    	
			int maxColumnId = row.getMaxColumnId();
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
	
	void put(List<Row> rows) throws IOException  {    	
    	getTableInfo(rows.get(0));
    	
    	// skip data from already dropped table    	
    	if (this.tableName == null) {
    		return;
    	}
    	
    	ArrayList<Put> puts = new ArrayList<Put>(rows.size());
    	
    	for (Row row : rows) {
    		
	        // populate row key    	
			byte[] key = Helper.antsKeyToHBase(row.getKeyAddress());
	    	Put put = new Put(key);
	
	    	// populate version
	    	
	    	long version = this.humpback.getTrxMan().getTimestamp(Row.getVersion(row.getAddress()));
	    	if (version < 0) {
	    		throw new OrcaHBaseException("invalid version {}", version);
	    	}
			put.addColumn(Helper.SYS_COLUMN_FAMILY_BYTES, Helper.SYS_COLUMN_VERSION_BYTES, 
							version, Bytes.toBytes(version));
	    	
			// populate size
			
			put.addColumn(Helper.SYS_COLUMN_FAMILY_BYTES, Helper.SYS_COLUMN_SIZE_BYTES, 
							version, Bytes.toBytes(row.getLength()));
			
	    	// populate fields
			
			int maxColumnId = row.getMaxColumnId();
	        for (int i=0; i<=maxColumnId; i++) {
	        	long pValue = row.getFieldAddress(i); 
				byte[] qualifier = tableColumnQualifierList.get(i);
	    		if (qualifier == null) {
	    			continue;
	    		}
	    		byte[] value = Helper.toBytes(pValue);
	    		put.addColumn(Helper.DATA_COLUMN_FAMILY_BYTES, qualifier, version, value);
	        }
	
	        // populate data types
			put.addColumn(Helper.SYS_COLUMN_FAMILY_BYTES, Helper.SYS_COLUMN_DATATYPE_BYTES, 
							version, this.tableColumnTypes);
			
			puts.add(put);
    	}

		Table htable = this.hbaseConn.getTable(this.tableName);
		htable.put(puts);
    }
}
