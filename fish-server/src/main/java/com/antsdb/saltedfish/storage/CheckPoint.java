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
	
	/** should be same as serverId from Humpback. used to prevent accidental sync*/
	long serverId;
	
	Connection conn;
    private boolean isMutable;
	
	CheckPoint(Humpback humpback, Connection conn, boolean isMutable) throws IOException {
		this.conn = conn;
		this.isMutable = isMutable;
		readFromHBase(humpback);
	}
	
	public long getCurrentSp() {
		return currentSp;
	}

	public void updateLogPointer(long lp) throws IOException {
        if (!this.isMutable) {
            throw new OrcaHBaseException("failed to update since it is realy only mode");
        }
	    updateHBase(this.conn, lp, this.serverId);
	    this.currentSp = lp;
	}
	
	public long getServerId() {
		return serverId;
	}

	public void setServerId(long value) {
		this.serverId = value;
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
	    if (!this.isMutable) {
	        throw new OrcaHBaseException("failed to update since it is realy only mode");
	    }
		updateHBase(this.conn, getCurrentSp(), getServerId());
	}

	private static void updateHBase(Connection conn, long currentSp, long serverId) throws IOException {
		// Get table object
		Table table = conn.getTable(TableName.valueOf(OrcaConstant.SYSNS, TABLE_SYNC_PARAM));
		
		// Generate put data
		Put put = new Put(KEY);
	    put.addColumn(COLUMN_FAMILY, Bytes.toBytes("currentSp"), Bytes.toBytes(currentSp));
	    put.addColumn(COLUMN_FAMILY, Bytes.toBytes("serverId"), Bytes.toBytes(serverId));
	    
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
