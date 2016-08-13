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
import java.util.List;

import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.util.UberUtil;

public class HBaseStorageSyncBuffer {
	static Logger _log = UberUtil.getThisLogger();
	
	HBaseStorageService hbaseService = null;
	int bufferSize = 1000;

	List<RowData> rowList = new ArrayList<RowData>();
	List<IndexData> indexList = new ArrayList<IndexData>();
	
	long currentSP = 0;
	long bufferedSP = -1;
	
	long putRowCount = 0;
	long indexRowCount = 0;
	long startTrxId;
	
	public HBaseStorageSyncBuffer(HBaseStorageService hbaseService, long currentSP, int bufferSize) {
		this.hbaseService = hbaseService;
		this.bufferSize = bufferSize;
		this.currentSP = currentSP; 
		this.bufferedSP = currentSP;
	}
	
	public void putRow(Row row, long sp) throws Exception {

		// check whether to flush
		checkFlush();

		// add to row buffer
		RowData rowData = new RowData(row, sp);
		rowList.add(rowData);
	}
	
	public void putIndex(int tableid, long trxts, long rowKey, long indexKey, long sp) throws Exception {

		// check whether to flush
		checkFlush();
		
		// add to index buffer
		IndexData idx = new IndexData(tableid, trxts, rowKey, indexKey, sp);
		indexList.add(idx);
	}
		
	public boolean rowExists(int tableid, long pkey) {
		byte[] key = com.antsdb.saltedfish.cpp.Bytes.get(null, pkey);
		
		Row row;
		for (RowData i : rowList) {
			row = i.getRow();
			if (row.getTableId() == tableid && key.equals(row.getKey())) {
				return true;
			}
		}

		// check for index
		for (IndexData i : indexList) {
			if (i.getTableId() == tableid) {
				byte[] rowKey = com.antsdb.saltedfish.cpp.Bytes.get(null, i.getRowKey());
				if (key.equals(rowKey)) {
					return true;
				}
			}
		}
		
		return false;
	}
	
	public void delete(int tableid, long pkey) {
		byte[] key = com.antsdb.saltedfish.cpp.Bytes.get(null, pkey);
		
		Row row;
		for (int i=rowList.size() - 1; i>=0; i--) {
			row = rowList.get(i).getRow();
			if (row.getTableId() == tableid && key.equals(row.getKey())) {
				rowList.remove(i);
			}
		}
	}
	
	public long getPutRowCount() {
		return this.putRowCount;
	}
	
	public long getIndexRowCount() {
		return this.indexRowCount;
	}
	
	public long getCurrentSP() {
		return this.currentSP;
	}
	
	public long getBufferedSP() {
		return this.bufferedSP;
	}

	public void setBufferedSP(long sp) {
		if (bufferedSP < sp) bufferedSP = sp;
		if (currentSP > sp) currentSP = sp;
	}
	
	private void checkFlush() throws Exception {
		// buffer full, we'll flush first
		if (rowList.size() + indexList.size() >= this.bufferSize) {
			
			flush();
		}
	}
	
	public void flush() throws Exception {
		
		try {
			// Save to hbase table
			flushRowBuffer();
		}
		catch(Exception ex) {
			// _log.error("failed to flush rows to hbase - sp: {} ~ {}", currentSP, bufferedSP, ex);
			throw new Exception(String.format("faield to flush rows to hbase - sp:%1$d ~ %2$d.", 
													currentSP, bufferedSP), ex);
		}
		
		try {
			flushIndexBuffer();
		}
		catch(Exception ex) {
			// _log.error("failed to flush index to hbase - sp: {} ~ {}", currentSP, bufferedSP, ex);
			throw new Exception(String.format("faield to flush index to hbase - sp:%1$d ~ %2$d.", 
													currentSP, bufferedSP), ex);
		}

		// write current SP to hbase
		if (currentSP != this.bufferedSP) {
			hbaseService.setCurrentSP(this.bufferedSP);
			currentSP = bufferedSP;
		}
		
		// update start trxid
		hbaseService.startTrxId = this.startTrxId;

		// update row count
		putRowCount += rowList.size();
		indexRowCount += indexList.size();

		// reset buffer & sp
		rowList.clear();
		indexList.clear();
		
	}
	
    public void flushRowBuffer() throws IOException  {
    	if (rowList.size() == 0) return;

    	List<Integer> tableIds = new ArrayList<Integer>();
    	Row row;
    	for (RowData i : rowList) {
    		row = i.getRow();
    		if (!tableIds.contains(row.getTableId())) {
    			tableIds.add(row.getTableId());
    		}
    	}
    	
    	if (tableIds.size() == 1) {
    		hbaseService.put(this.rowList);
    	}
    	else {
	    	List<RowData> rowsInOneTable = new ArrayList<RowData>();
	    	for (Integer id : tableIds) {
	    		rowsInOneTable.clear();
	    		for (RowData i: this.rowList) {
	    			if (i.getRow().getTableId() == id) {
	    				rowsInOneTable.add(i);
	    			}
	    		}
	    		
				if (rowsInOneTable.size() > 0) {
					hbaseService.put(rowsInOneTable);
				}
	    	}
    	}
	}

    private void flushIndexBuffer() throws IOException  {
    	if (indexList.size() == 0) return;
    	
    	List<Integer> tableIds = new ArrayList<Integer>();
    	for (IndexData i : this.indexList) {
    		if (!tableIds.contains(i.getTableId())) {
    			tableIds.add(i.getTableId());
    		}
    	}
    	
    	if (tableIds.size() == 1) {
    		hbaseService.index(this.indexList);
    	}
    	else {
	    	List<IndexData> indics = new ArrayList<IndexData>();
	    	for (Integer id : tableIds) {
	    		indics.clear();
	    		for (IndexData i: this.indexList) {
	    			if (i.getTableId() == id) {
	    				indics.add(i);
	    			}
	    		}
	    		
				if (indics.size() > 0) {
					hbaseService.index(indics);
				}
	    	}
    	}
		this.hbaseService.index(indexList);
    }
    
    public class RowData {
    	private Row row;
    	private long sp;
    	public RowData(Row row, long sp) {
    		this.row = row;
    		this.sp = sp;
    	}
    	
    	public long getSp() {
    		return this.sp;
    	}
    	
    	public Row getRow() {
    		return this.row;
    	}
    }
    
	public class IndexData {
		private int tableid;
		private long trxts;
		private long rowKey;
		private long indexKey;
		private long sp;
		
		public IndexData(int tableid, long trxts, long rowKey, long indexKey, long sp) {
			this.tableid = tableid;
			this.trxts = trxts;
			this.rowKey = rowKey;
			this.indexKey = indexKey;
			this.sp = sp;
		}
		
		public int getTableId() {
			return this.tableid;
		}
		
		public long getTrxTs() {
			return this.trxts;
		}
		
		public long getRowKey() {
			return this.rowKey;
		}
		
		public long getIndexKey() {
			return this.indexKey;
		}

    	public long getSp() {
    		return this.sp;
    	}
	}
}
