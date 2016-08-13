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
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.sql.OrcaConstant;

/**
 * 
 * @author wgu0
 */
public class CheckPoint {
    final static String TABLE_SYNC_PARAM = "SYNCPARAM";
    final static byte[] COLUMN_FAMILY = Bytes.toBytes("d");
    final static byte[] KEY = Bytes.toBytes(0);
    final static byte[] TRUNCATE_KEY = Bytes.toBytes(1);
    
	/** space pointer that has been synchronized */
	volatile long currentSp;
	
	/** flag to prevent accidental concurrent process */
	boolean isOpen = false;

	/** should be same as serverId from Humpback. used to prevent accidental sync*/
	long serverId;
	
	/** save truncated table list and their corresponding Sps */
    ConcurrentMap<Integer, Long> truncateTableSpList = new ConcurrentHashMap<>();
	
	Connection conn;
	
	CheckPoint(Humpback humpback, Connection conn) throws IOException {
		this.conn = conn;
		readFromHBase(humpback);
	}
	
	public long getCurrentSp() {
		return currentSp;
	}

	public void setCurrentSp(long currentSp) {
		this.currentSp = currentSp;
	}

	public boolean isOpen() {
		return isOpen;
	}

	public void setOpen(boolean isOpen) {
		this.isOpen = isOpen;
	}

	public long getServerId() {
		return serverId;
	}

	public void setServerId(long value) {
		this.serverId = value;
	}
	
	// put into list first with a negative value
	public void setTruncateTableSp(int tableid, long sp) throws IOException {
		
		truncateTableSpList.put(tableid, sp);
		
		// clear expired
		checkAndClearTruncateTableSpList();
		
		// update hbase
		updateTruncateTableSpListToHBase();
		
	}
	
    public boolean isTruncatedData(int tableid, long sp) {
    	Long truncateSp = truncateTableSpList.get(tableid);
    	if (truncateSp == null) return false;
		return sp < truncateSp;
    }

	public void readFromHBase(Humpback humpback) throws IOException {
		// Get table object
		Table table = this.conn.getTable(TableName.valueOf(OrcaConstant.SYSNS, TABLE_SYNC_PARAM));
		
		// Query row
		Get get = new Get(KEY);
		Result result = table.get(get);
		if (!result.isEmpty()) {
			this.currentSp = Bytes.toLong(result.getValue(COLUMN_FAMILY, Bytes.toBytes("currentSp")));
			this.serverId = Bytes.toLong(result.getValue(COLUMN_FAMILY, Bytes.toBytes("serverId")));
			this.isOpen = Bytes.toBoolean(result.getValue(COLUMN_FAMILY, Bytes.toBytes("isOpen")));
			// load truncate table sp list
			byteArrayToTruncateTableSpList(result.getValue(COLUMN_FAMILY,  Bytes.toBytes("truncateTableSp")), 
								this.truncateTableSpList);
		}
		else {
			this.serverId = humpback.getServerId();
			this.currentSp = humpback.getGobbler().getStartSp();
		}
	}
	
	/**
	 * save changes to hbase
	 * @throws IOException 
	 */
	public void updateHBase() throws IOException {
		
		// remove all expired truncate table sp before updating to hbase
		checkAndClearTruncateTableSpList();
		
		updateHBase(this.conn, getCurrentSp(), this.isOpen, getServerId(), this.truncateTableSpList);
	}

	void readTruncateTableSpListFromHBase() throws IOException {
		// Get table object
		Table table = this.conn.getTable(TableName.valueOf(OrcaConstant.SYSNS, TABLE_SYNC_PARAM));
		
		// Query row
		Get get = new Get(KEY);
		Result result = table.get(get);
		byte[] value = null;
		if (!result.isEmpty()) {
			// load truncate table sp list
			value = result.getValue(COLUMN_FAMILY,  Bytes.toBytes("truncateTableSp"));
		}
		
		byteArrayToTruncateTableSpList(value, this.truncateTableSpList);
	}

	void updateTruncateTableSpListToHBase() throws IOException {
		// Get table object
		Table table = this.conn.getTable(TableName.valueOf(OrcaConstant.SYSNS, TABLE_SYNC_PARAM));
		
		// Generate put data
		Put put = new Put(KEY);
		
		byte[] value = truncateTableSpListToByteArray(this.truncateTableSpList);
		put.addColumn(COLUMN_FAMILY, Bytes.toBytes("truncateTableSp"), value);
		
		// Add row
		table.put(put);
	}

	void checkAndClearTruncateTableSpList() {
    	
    	if (truncateTableSpList.size() == 0) {
    		return;
    	}
    	
		// clear expired truncate list from memory and hbase
    	int id;
    	Iterator<Integer> it = truncateTableSpList.keySet().iterator();
    	while (it.hasNext()) {
    		id = it.next();
    		long truncateSp = truncateTableSpList.get(id);
    		if (truncateSp > 0 && truncateSp <= this.getCurrentSp()) {
    			// remove from memory
    			it.remove();
    		}
    	}    	
    }
    
	static void byteArrayToTruncateTableSpList(byte[] byteArray, ConcurrentMap<Integer, Long> list) {
		list.clear();
		if (byteArray == null || byteArray.length == 0) 
			return;
		
		int tableid;
		long sp;
		int offset=0;
		while (offset < byteArray.length - (Integer.SIZE + Long.SIZE)) {
			tableid = Bytes.toInt(byteArray, offset);
			offset += Integer.SIZE;
			sp = Bytes.toLong(byteArray, offset);
			offset += Long.SIZE;
			list.put(tableid, sp);
		}
	}
	
	static byte[] truncateTableSpListToByteArray(ConcurrentMap<Integer, Long> list) {
		if (list.size() == 0) return null;
		
		ByteBuffer buf = ByteBuffer.allocate(list.size() * (Integer.SIZE + Long.SIZE));
		for (Entry<Integer, Long> i : list.entrySet()) {
			buf.putInt(i.getKey());
			buf.putLong(i.getValue());
		}
		return buf.array();
	}	

	public static void updateHBase(Connection conn, long currentSp, boolean isOpen, long serverId,
			ConcurrentMap<Integer, Long> truncateTableSpList) throws IOException {
		// Get table object
		Table table = conn.getTable(TableName.valueOf(OrcaConstant.SYSNS, TABLE_SYNC_PARAM));
		
		// Generate put data
		Put put = new Put(KEY);
	    put.addColumn(COLUMN_FAMILY, Bytes.toBytes("currentSp"), Bytes.toBytes(currentSp));
	    put.addColumn(COLUMN_FAMILY, Bytes.toBytes("isOpen"), Bytes.toBytes(isOpen));
	    put.addColumn(COLUMN_FAMILY, Bytes.toBytes("serverId"), Bytes.toBytes(serverId));
	    
	    if (truncateTableSpList != null) {
	    	byte[] value = truncateTableSpListToByteArray(truncateTableSpList);
	    	put.addColumn(COLUMN_FAMILY, Bytes.toBytes("truncateTableSp"), value);
	    }
		// Add row
		table.put(put);
	}
	
	public static long readCurrentSPFromHBase(Connection conn) throws IOException {
		// Get table object
		Table table = conn.getTable(TableName.valueOf(OrcaConstant.SYSNS, TABLE_SYNC_PARAM));
		long currentSp = -1;
		// Query row
		Get get = new Get(KEY);
		Result result = table.get(get);
		if (!result.isEmpty()) {
			currentSp = Bytes.toLong(result.getValue(COLUMN_FAMILY, Bytes.toBytes("currentSp")));
		}
		return currentSp;
	}
	
	public static void udpateCurrentSPToHBase(Connection conn, long currentSp) throws IOException {
		// Get table object
		Table table = conn.getTable(TableName.valueOf(OrcaConstant.SYSNS, TABLE_SYNC_PARAM));
		
		// Generate put data
		Put put = new Put(KEY);
	    put.addColumn(COLUMN_FAMILY, Bytes.toBytes("currentSp"), Bytes.toBytes(currentSp));
		
		// Add row
		table.put(put);
	}
}
