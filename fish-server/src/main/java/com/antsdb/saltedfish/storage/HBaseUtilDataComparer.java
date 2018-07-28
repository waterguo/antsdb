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
package com.antsdb.saltedfish.storage;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaConstant;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.util.UberUtil;

public class HBaseUtilDataComparer {

   static Logger _log = UberUtil.getThisLogger();

	static final int ROWS_PER_PUT = 100;
	static final long REPORT_INTERVAL = 100000;
	
	int testRows = 100;
	Orca orca = null;
	Connection hbaseConn = null;
	Humpback humpback = null;
	MetadataService metaService = null;
	String[] skipTables = null;
	String[] syncTables = null;
	boolean ignoreError = false;
	
	public HBaseUtilDataComparer(Orca orca, Connection hbaseConn, int testRows, 
			String[] skipTables, String[] syncTables, boolean ignoreError) {
		this.orca = orca;
		this.hbaseConn = hbaseConn;
		this.testRows = testRows;
		this.humpback = orca.getHumpback();
		this.metaService = orca.getMetaService();
		// get skip tables list
		if (skipTables != null && !skipTables[0].isEmpty()) this.skipTables = skipTables;	
		if (syncTables != null && !syncTables[0].isEmpty()) this.syncTables = syncTables;
	
		this.ignoreError = ignoreError;
	}
	
	long totalTables = 0, totalSeconds = 0;
	List<String> successTableList = new ArrayList<String>();
	List<String> failureTableList = new ArrayList<String>();
	List<String> skipTableList = new ArrayList<String>();
	String importResult;
	long count;
	public void run() throws Exception {
		
		long testStart = System.currentTimeMillis(); 
		try {
			TableDataComparer comparer = new TableDataComparer(this.hbaseConn, this.testRows);

			List<String> namespaces = humpback.getNamespaces();
			for (String ns : namespaces) {
	
				// get table list from antsdb				
				Collection<GTable> tables = humpback.getTables(ns);
	
				for (GTable table : tables) {
	
					TableName tableName = getTableName(table.getId());
					
					try {
						
						// check whether in sync tables
						if (isTableToTest(tableName)) {
							
							totalTables++;
							
							System.out.printf("[%s] - ", tableName.toString());
							
							if (isTableSkipped(tableName)) {
								
								skipTableList.add(tableName.toString());
								
								System.out.println(">>>> SKIPPED");								
								continue;
							}
	
							if (comparer.doCompare(table)) {
								successTableList.add(tableName.toString());
							}
							else if (!comparer.isSkipped()) {
								failureTableList.add(tableName.toString());
							}
								
							// print result
							System.out.print("Total rows: " + comparer.getTotalRowCount());
							if (comparer.getErrorRowCount() > 0) {
								System.out.print("     failed rows: " + comparer.getErrorRowCount());
							}
							System.out.println("");
						}
					}
					catch (Exception e) {
	
						if (!failureTableList.contains(tableName.toString())) {
							failureTableList.add(tableName.toString());
						}

						System.out.println("Failed - " + e.getMessage());
						e.printStackTrace();
						
						if (!ignoreError) throw e;
					}
				}				
			}
		}
		catch (Exception e) {
			throw e;
		}
		
		long testEnd = System.currentTimeMillis();
		totalSeconds = (testEnd - testStart) / 1000;
	}
	
	public String getResult() {
		return String.format("HBase data check finished. Total time: %1$d seconds\n" +
				"Total Tables: %2$d [Success: %3$d    Skipped: %4$d    Failed: %5$d]\n", 
				totalSeconds, totalTables,
				successTableList.size(), skipTableList.size(), failureTableList.size());
	}
	
	private boolean isTableSkipped(TableName tableName) {
		String name = tableName.toString();
		if (this.skipTables != null) {
			for (String i:this.skipTables) {
				if (i.compareToIgnoreCase(name) == 0)
					return true;
			}
		}
		
		return false;
	}
	
	private boolean isTableToTest(TableName tableName) {
		
		if (this.syncTables == null || 
				this.syncTables.length == 0) {
			return true;
		}
		
		String name = tableName.toString();
		for (String i:this.syncTables) {
			if (i.compareToIgnoreCase(name) == 0)
				return true;
		}
		
		return false;
	}
	
	static final char[] hexArray = "0123456789ABCDEF".toCharArray();
	public static String bytesToHex(byte[] bytes) {
		if (bytes == null) return "(null)";
		if (bytes.length == 0) return "(0 length)";
		
	    char[] hexChars = new char[bytes.length * 4];
	    for ( int j = 0; j < bytes.length; j++ ) {
	        int v = bytes[j] & 0xFF;
	        hexChars[j * 4] = '\\';
	        hexChars[j * 4 + 1] = 'x';
	        hexChars[j * 4 + 2] = hexArray[v >>> 4];
	        hexChars[j * 4 + 3] = hexArray[v & 0x0F];
	    }
	    return new String(hexChars);
	}

	TableMeta getTableMeta(int tableId) {
		if (this.metaService == null) {
			return null;
		}
		if (tableId < 0) {
			return null;
		}
    	return this.metaService.getTable(Transaction.getSeeEverythingTrx(), tableId);
	}
	
	private TableName getTableName(int tableId) {
		if (tableId < 0) {
	    	String name = String.format("%08x", tableId);
			return TableName.valueOf(OrcaConstant.SYSNS, name);
		}
		SysMetaRow metarow = this.humpback.getTableInfo(tableId);
		return (metarow != null) ? TableName.valueOf(metarow.getNamespace(), metarow.getTableName()) : null;
	}

	class TableDataComparer {
		Connection hbaseConn = null;
		GTable table;
		long compareRows = 0;
		int tableId;
		TableName tableName;
		TableMeta tableMeta;
		
		List<byte[]> tableColumnQualifierList = new ArrayList<byte[]>();
		byte[] tableColumnTypes = null;
		
		boolean isIndexTable = false;
		long totalRowCount = 0;
		long errorRowCount = 0;
		boolean skipped = false;		

		public TableDataComparer(Connection hbaseConn, long compareRows) {
			this.hbaseConn = hbaseConn;
			this.compareRows = compareRows;
		}
		
		public long getTotalRowCount() {
			return this.totalRowCount;
		}
		
		public long getErrorRowCount() {
			return this.errorRowCount;
		}
		
		public boolean isSkipped() {
			return skipped;
		}
		
		public boolean doCompare(GTable table) throws Exception {
			
			// reset all interval variables for this table
			
			this.table = table;
			this.tableId = table.getId();
			this.tableName = getTableName(tableId);
			this.tableMeta = getTableMeta(tableId);
			
			this.isIndexTable = false;
			this.totalRowCount = 0;
			this.errorRowCount = 0;
			
			this.tableColumnQualifierList.clear();
			this.tableColumnTypes = null;

			this.skipped = false;
			
			// check existence of hbase table
			
			if (!Helper.existsTable(this.hbaseConn, 
						tableName.getNamespaceAsString(), tableName.getQualifierAsString())) {
				System.out.println("Table not exists in HBase!");
				return false;
			}

			// Scanner for antsdb
			RowIterator scanner;
			scanner = table.scan(0, Long.MAX_VALUE, 0, 0, 0);
			
			// check if it's index table
			if (scanner.next()) {
				this.isIndexTable = table.getTableType() == TableType.INDEX;
			}
			else {
				// no rows, return directly
				return true;
			}

			boolean result = true;
			int getRowKeyException = 0;
			
			// open hbase table
			Table htable = this.hbaseConn.getTable(tableName);
			List<Get> getList = new ArrayList<Get>();

			boolean anyRowLeft;
			
			if (isIndexTable) {
				List<IndexDataRow> indexRows =  new ArrayList<IndexDataRow>(ROWS_PER_PUT);
				long indexKeyPointer, rowKeyPointer, version;
				IndexDataRow indexRow;
				
				System.out.print("INDEX Table   ");

				do {
					this.totalRowCount++;
					indexKeyPointer = scanner.getKeyPointer();
					rowKeyPointer = scanner.getRowKeyPointer();
					version = scanner.getVersion();
					indexRow = new IndexDataRow(rowKeyPointer, indexKeyPointer, version);
					indexRows.add(indexRow);

					// move to next row
					anyRowLeft = scanner.next();
					
					// check 1000 rows in a batch 
					if (!anyRowLeft || indexRows.size() >= 1000) {
					
						for (IndexDataRow i : indexRows) {
							byte[] key = Helper.antsKeyToHBase(i.getIndexKeyPointer());
							Get get = new Get(key);
							getList.add(get);
						}
	
						// retrieve data from hbase
						Result[] resultList = htable.get(getList);

						// Compare each row with existing in row list
						for (int i=0; i<indexRows.size(); i++) {
							indexRow = indexRows.get(i);
							
							try {
								compareOneIndexRow(indexRow, resultList[i]);
							}
							catch(Exception ex) {
								result = false;
								System.out.println(String.format("Failed in comparing Index - %1$d: %2$s", 
													this.totalRowCount - indexRows.size() + i + 1, ex.getMessage()));

								errorRowCount ++;
								
								// stops if error occurs more than 10 times and not to ignoreError 
								if (!ignoreError || this.errorRowCount >= 10) {
									if (this.errorRowCount >= 10) {
										System.out.println("Error rows exceeding 10... stopped comparing on table " + 
															tableName.toString());
									}
									throw ex;
								}
							}
						}
						
						// Clear for next batch						
						indexRows.clear();
						getList.clear();
					}
					// end of scanner
					if (this.compareRows > 0 && this.totalRowCount >= this.compareRows) break;
					
				} while (anyRowLeft);
			}
			else {
				List<Row> rows =  new ArrayList<Row>();
				Row row;

				do {
					this.totalRowCount++;
					row = scanner.getRow();
					rows.add(row);

					// is row key oofset valid? 
					if (getRowKeyException < 10) {
						try {
							row.getKey();
						}
						catch (IllegalArgumentException ex) {
							System.out.println("Row.getKey IllegalArgumentException - sp = " + row.getAddress());
							getRowKeyException ++;
						}
						catch (Exception ex) {
							System.out.println("Failed to get antskey - " + ex.getMessage());
						}
					}

					// move to next row
					anyRowLeft = scanner.next();
					
					// check 1000 rows in a batch 
					if (!anyRowLeft || rows.size() >= 1000) {
						for (Row i : rows) {
							byte[] key = Helper.antsKeyToHBase(i.getKeyAddress());
							Get get = new Get(key);
							getList.add(get);
						}
	
						// retrieve data from hbase
						Result[] resultList = htable.get(getList);
						
						// Compare each row with existing in row list
						for (int i=0; i<rows.size(); i++) {
							row = rows.get(i);
							this.updateColumnInfo(tableMeta, row);
							
							try {
								compareOneRow(tableMeta, row, resultList[i]);
							}
							catch(Exception ex) {
								result = false;
								System.out.println(String.format("Failed in comparing row - %1$d: %2$s", 
													this.totalRowCount - rows.size() + i + 1, ex.getMessage()));
								//System.out.println(row.toString());
								
								errorRowCount ++;
								
								// stops if error occurs more than 10 times and not to ignoreError 
								if (!ignoreError || this.errorRowCount >= 10) {
									if (this.errorRowCount >= 10) {
										System.out.println("Error rows exceeding 10... stopped comparing on table " + 
															tableName.toString());
									}
									throw ex;
								}
							}
						}
						
						// Clear for next batch						
						rows.clear();
						getList.clear();
					}
					
					// end of scanner
					if (this.compareRows > 0 && this.totalRowCount >= this.compareRows) break;
					
				} while (anyRowLeft);
				
			}
			
			htable.close();
			
			return result;
		}

		void updateColumnInfo(TableMeta tableMeta, Row row) {
			
			int maxColumnId = row.getMaxColumnId();
			
			if (this.tableColumnQualifierList.size() < maxColumnId + 1) {
				
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
		
		void compareOneIndexRow(IndexDataRow row, Result r) throws Exception{

			byte[] indexKey = Helper.antsKeyToHBase(row.getIndexKeyPointer());

			// populate row key
	    	byte[] rowKey = Helper.antsKeyToHBase(row.getRowKeyPointer());

			if (r == null || r.isEmpty()) {
				throw new Exception("Index Row can't be found in hbase - key: " + bytesToHex(indexKey));
			}
			
			byte[] key = Helper.hbaseKeyToAnts(r.getRow());
			if (!Arrays.equals(key, indexKey)) {
				throw new Exception("Row key not match - ANTS: " + bytesToHex(indexKey) + ", HBASE: " + bytesToHex(key));
			}
			
			// hbase data		
		    NavigableMap<byte[], byte[]> sysFamilyMap = r.getFamilyMap(Helper.DATA_COLUMN_FAMILY_BYTES);
	    	// index key
		    byte[] rowKeyBytes = sysFamilyMap.get(Helper.SYS_COLUMN_INDEXKEY_BYTES);
	    	// version
		    //byte[] versionBytes = sysFamilyMap.get(Helper.SYS_COLUMN_VERSION_BYTES);
		    //long version = Bytes.toLong(versionBytes);
		    // if (version != )
		    
	    	if (!Arrays.equals(rowKey,  rowKeyBytes)) {
	    		throw new Exception(String.format("Index [%1$s] row key not match ANTS:[%2$s] HBASE:[%3$s]",
							bytesToHex(key), bytesToHex(rowKey), bytesToHex(rowKeyBytes)));
	        }
		}
		
		void compareOneRow(TableMeta tableMeta, Row row, Result r) throws Exception{
		
			if (row == null) {
				throw new Exception("row is null");
			}
			
			byte[] antsKey = KeyBytes.create(row.getKeyAddress()).get();	
			if (r == null || r.isEmpty()) {
				throw new Exception("Row can't be found in hbase - key: " + bytesToHex(antsKey));
			}
			
			// some preparation
			
	//	    NavigableMap<byte[], byte[]> sysFamilyMap = r.getFamilyMap(Helper.SYS_COLUMN_FAMILY_BYTES);
		    NavigableMap<byte[], byte[]> dataFamilyMap = r.getFamilyMap(Helper.DATA_COLUMN_FAMILY_BYTES);
	//	    byte[] colDataType = sysFamilyMap.get(Helper.SYS_COLUMN_DATATYPE_BYTES);
	//	    byte[] versionBytes = sysFamilyMap.get(Helper.SYS_COLUMN_VERSION_BYTES);
	//	    byte[] sizeBytes = sysFamilyMap.get(Helper.SYS_COLUMN_SIZE_BYTES);
	//	    long version = Bytes.toLong(versionBytes);
	//	    int size = Bytes.toInt(sizeBytes);
		    
			byte[] key = Helper.hbaseKeyToAnts(r.getRow());
			if (!Arrays.equals(key, antsKey)) {
				throw new Exception("Row key not match - ANTS: " + bytesToHex(antsKey) + ", HBASE: " + bytesToHex(key));
			}
			
			int maxColumnId = row.getMaxColumnId();
	//		byte[] types = new byte[maxColumnId+1];
			String errMsg = "";
	        for (int i=0; i<=maxColumnId; i++) {
				byte[] qualifier = tableColumnQualifierList.get(i);
	    		if (qualifier == null) {
	    			continue;
	    		}
	    		
	        	long pValue = row.getFieldAddress(i); 
	    		byte[] antsValue = Helper.toBytes(pValue);
	    		byte[] value = dataFamilyMap.get(qualifier);
	    		
	    		if (!Arrays.equals(antsValue,  value)) {
	    			String columnName = new String(qualifier, StandardCharsets.US_ASCII);
	    			if (errMsg.length() == 0) {
	    				errMsg += "Row Key=[" + bytesToHex(key) + "]";
	    			}
	    			errMsg += String.format("\n    Column %1$d '%2$s'[%3$s] not match - ANTS:[%4$s] HBASE:[%5$s]",
							i, columnName, bytesToHex(qualifier), bytesToHex(antsValue), bytesToHex(value));
	    		}
	        }
	        
	        if (errMsg != "") {
	        	throw new Exception(errMsg);
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
			
		public class IndexDataRow {
			long rowKeyPointer;
			long indexKeyPointer;
			long version;
			
			public IndexDataRow(long pRowKey, long pIndexKey, long version) {
				this.rowKeyPointer = pRowKey;
				this.indexKeyPointer = pIndexKey;
				this.version = version;
			}
			
			public long getRowKeyPointer() {
				return rowKeyPointer;
			}
			
			public long getIndexKeyPointer() {
				return indexKeyPointer;
			}
			
			public long getVersion() {
				return version;
			}
		}
	}
}
