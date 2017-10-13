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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaConstant;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.util.UberUtil;

public class HBaseUtilImporter {
    static Logger _log = UberUtil.getThisLogger();

	static final int ROWS_PER_PUT = 500;
	static final long REPORT_INTERVAL = 100000;
	
	int threadCount = 5;
	Orca orca = null;
	Connection hbaseConn = null;
	Humpback humpback = null;
	MetadataService metaService = null;
	String[] skipTables = null;
	String[] syncTables = null;
	boolean ignoreError = false;
	Algorithm compressionType = Algorithm.GZ;
	int columnsPerPut = 2000;
	
	public HBaseUtilImporter(int threadCount, Orca orca, Connection hbaseConn, 
					String[] skipTables, String[] syncTables, boolean ignoreError) {
		this.threadCount = threadCount;
		this.orca = orca;
		this.hbaseConn = hbaseConn;
		this.humpback = orca.getHumpback();
		this.metaService = orca.getMetaService();
		// get skip tables list
		if (skipTables != null && !skipTables[0].isEmpty()) this.skipTables = skipTables;	
		if (syncTables != null && !syncTables[0].isEmpty()) this.syncTables = syncTables;

		this.ignoreError = ignoreError;
	}
	
	public void setCompressionType(Algorithm compressionType) {
		this.compressionType = compressionType;
	}
	
	public void setColumnsPerPut(int columns) {
		this.columnsPerPut = columns;
	}
	
	long totalTables = 0, totalSeconds = 0;
	long totalRows = 0;
	long totalIndexRows = 0;
	List<String> successTableList = new ArrayList<String>();
	List<String> failureTableList = new ArrayList<String>();
	List<String> skipTableList = new ArrayList<String>();
	
	String importResult;
	long currentSP = -1;
	public void run() throws Exception {
		
		long importStart = System.currentTimeMillis(); 
		System.out.println("HBase import starts - compression: " + this.compressionType.toString() + 
							", " + this.columnsPerPut + " columns/put");

		// create background threads
		HBaseImportThreadManager importMgr = new HBaseImportThreadManager(this.threadCount, this.hbaseConn, 
											this.humpback, this.metaService, ignoreError, this.columnsPerPut);
		
		long start, end, count;
		try {
			// init hbase
			System.out.println("*********** Check and initialize hbase DB for antsdb ***********\n");
			initHBaseDB(this.hbaseConn);

			// start background threads
			importMgr.start();
		
			List<String> namespaces = humpback.getNamespaces();
			for (String ns : namespaces) {

				// get table list from antsdb				
				Collection<GTable> tables = humpback.getTables(ns);
	
				for (GTable table : tables) {

					TableName tableName = getTableName(table.getId());
					
					try {
						
						// check whether in sync tables
						if (isTableToSync(tableName)) {
							
							totalTables++;
							
							start = System.currentTimeMillis();
							
							System.out.printf("[%s]", tableName.toString());
							
							if (isTableSkipped(tableName)) {
								
								skipTableList.add(tableName.toString());
								
								System.out.println(" >>>> SKIPPED");								
								continue;
							}

							count = importOneTable(importMgr, table, REPORT_INTERVAL);
							
							end = System.currentTimeMillis();
							System.out.printf("\n    Total: %,d rows,  %.3f seconds.\n", 
													count, (end - start) * 1.0f / 1000);
						}
					}
					catch (Exception e) {

						failureTableList.add(tableName.toString());
						
						System.out.println("    Failed !");
						e.printStackTrace();
						
						if (!ignoreError) throw e;
					}
				}				
			}
			
			// wait until all threads done rows in buffer
			importMgr.waitForFinish();
		}
		catch (Exception e) {
			
			// inform the background threads to terminate
			importMgr.shutdown();
			
			throw e;
			
		}
		
		// write current SP to SYNCPARAM
		currentSP = this.humpback.getLatestSP();
		System.out.println("*******************************************************************");
		System.out.printf("Update SYNCPARAM to %d\n", currentSP);
		System.out.println("*******************************************************************\n");
		CheckPoint cp = new CheckPoint(humpback, this.hbaseConn, true);
		cp.setServerId(this.humpback.getServerId());
		cp.updateLogPointer(currentSP);
    	
		long importEnd = System.currentTimeMillis();
		totalSeconds = (importEnd - importStart) / 1000;
    	//System.out.println("-------------------------------------------------------------------");
		//System.out.println(getResult());
		//System.out.println("-------------------------------------------------------------------\n");
	}
	
	public String getResult() {
		return String.format("HBase import finished. Total time: %1$d seconds\n" +
				"Total Tables: %2$d [Success: %3$d    Skipped: %4$d    Failed: %5$d]\n" +
				"Total Rows: %6$d [Table Rows: %7$d    Index Rows: %8$d], %9$d rows/second\n" +
				"Current SP set to %10$d", totalSeconds, totalTables,
				successTableList.size(), skipTableList.size(), failureTableList.size(), 
				totalRows, totalRows - totalIndexRows, totalIndexRows, 
				(totalSeconds > 0) ? totalRows / totalSeconds : "N/A",
				currentSP);		
	}

    private void initHBaseDB(Connection conn) throws OrcaHBaseException {
    	if (!Helper.existsNamespace(conn, OrcaConstant.SYSNS)) {
    		_log.info("Namespace " + OrcaConstant.SYSNS + " is not found in HBase, creating ...");
    		Helper.createNamespace(conn, OrcaConstant.SYSNS);
    	}
    	if (!Helper.existsTable(conn, OrcaConstant.SYSNS, CheckPoint.TABLE_SYNC_PARAM)) {
    		_log.info("Checkpoint table " + CheckPoint.TABLE_SYNC_PARAM + 
    					" is not found in HBase, creating ...");
    		Helper.createTable(conn, OrcaConstant.SYSNS, CheckPoint.TABLE_SYNC_PARAM, this.compressionType);
    	}
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
	
	private boolean isTableToSync(TableName tableName) {
		
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
	
	private long importOneTable(HBaseImportThreadManager importMgr, 
							GTable table, long reportInterval) throws Exception {

		int tableId = table.getId();
		TableName tableName = getTableName(tableId);
		long count = 0;
		List<Row> rows = null;
 
		// create hbase table
    	if (!Helper.existsTable(this.hbaseConn, tableName.getNamespaceAsString(), tableName.getQualifierAsString())) {
			Helper.createTable(this.hbaseConn, tableName.getNamespaceAsString(), 
							tableName.getQualifierAsString(), this.compressionType);
			System.out.print(", CREATED");
    	}
    	
		// import table rows
		count = 0;
		rows =  new ArrayList<Row>(ROWS_PER_PUT);

		RowIterator scanner = table.scan(0, Long.MAX_VALUE);
		
		boolean isIndexTable = table.getTableType() == TableType.INDEX;
		boolean firstRow = true;
		while (scanner.next()) {
			// use first row to check whether index table
			if (firstRow) {
				firstRow = false;

				if (isIndexTable) {
					System.out.print(", INDEX  ");
					break;
				}
				else {
					System.out.print("  ");
				}
			}

			Row row = scanner.getRow();
			rows.add(row);
			
			if (rows.size() >= ROWS_PER_PUT) {
		
				TableRows r = new TableRows(tableId, rows, 0);
				importMgr.putRows(r);						
				count += rows.size();
				
				if ((count % reportInterval) == 0) {
					System.out.print(".");
					// _log.info("Done rows: " + count);
				}
				
				rows = new ArrayList<Row>(ROWS_PER_PUT);
			}
		}

		if (rows.size() > 0) {
			TableRows r = new TableRows(tableId, rows, 0);
			importMgr.putRows(r);						
			count += rows.size();
		}
		
		// process index table
		
		if (isIndexTable) {
			List<IndexDataRow> indexRows =  new ArrayList<IndexDataRow>(ROWS_PER_PUT);
			
			long indexKeyPointer, rowKeyPointer, version;
			IndexDataRow indexRow;
			do {
				indexKeyPointer = scanner.getKeyPointer();
				rowKeyPointer = scanner.getRowKeyPointer();
				version = scanner.getVersion();
				indexRow = new IndexDataRow(rowKeyPointer, indexKeyPointer, version);
				indexRows.add(indexRow);
				
				if (indexRows.size() >= ROWS_PER_PUT) {
			
					TableRows r = new TableRows(tableId, indexRows);
					importMgr.putRows(r);						
					count += indexRows.size();
					
					if ((count % reportInterval) == 0) {
						System.out.print(".");
						// _log.info("Done rows: " + count);
					}
					
					indexRows = new ArrayList<IndexDataRow>(ROWS_PER_PUT);
				}
			} while (scanner.next()); 

			if (indexRows.size() > 0) {
				
				TableRows r = new TableRows(tableId, indexRows);
				importMgr.putRows(r);
				count += indexRows.size();
			}				
		}
		
		successTableList.add(tableName.toString());
		
		totalRows += count;
		if (isIndexTable) totalIndexRows += count;
		
		return count;
	}
	
	private TableName getTableName(int tableId) {
    	if (tableId < 0) {
        	String name = String.format("%08x", tableId);
    		return TableName.valueOf(OrcaConstant.SYSNS, name);
    	}
    	SysMetaRow metarow = this.humpback.getTableInfo(tableId);
    	return (metarow != null) ? TableName.valueOf(metarow.getNamespace(), metarow.getTableName()) : null;
	}

	public class TableRows {
		boolean index = false;
		int tableId;
		List<IndexDataRow> indexRows = null;
		List<Row> rows = null;
		
		public TableRows(int tableId, List<Row> rows, int nothing) {
			this.index = false;
			this.tableId = tableId;
			this.rows = rows;
			this.indexRows = null;
		}
		
		public TableRows(int tableId, List<IndexDataRow> indexRows) {
			this.index = true;
			this.tableId = tableId;
			this.rows = null;
			this.indexRows = indexRows;
		}
		
		public boolean isIndex() {
			return index;
		}
		
		public List<Row> getRows() {
			return rows;
		}
		
		public int getTableId() {
			return this.tableId;
		}
		
		public List<IndexDataRow> getIndexRows() {
			return this.indexRows;
		}
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
