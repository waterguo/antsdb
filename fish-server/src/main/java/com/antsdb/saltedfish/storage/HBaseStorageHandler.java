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

import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.Gobbler;
import com.antsdb.saltedfish.nosql.Gobbler.CommitEntry;
import com.antsdb.saltedfish.nosql.Gobbler.DeleteEntry;
import com.antsdb.saltedfish.nosql.Gobbler.IndexEntry;
import com.antsdb.saltedfish.nosql.Gobbler.LogEntry;
import com.antsdb.saltedfish.nosql.Gobbler.MessageEntry;
import com.antsdb.saltedfish.nosql.Gobbler.RollbackEntry;
import com.antsdb.saltedfish.nosql.Gobbler.TransactionWindowEntry;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.ReplayHandler;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.SpaceManager;
import com.antsdb.saltedfish.nosql.TrxMan;
import com.antsdb.saltedfish.util.JumpException;
import com.antsdb.saltedfish.util.UberUtil;

public class HBaseStorageHandler extends ReplayHandler {
	static Logger _log = UberUtil.getThisLogger();
	
	Humpback humpback;
	SpaceManager spaceman;
	HBaseStorageService hbaseStorageService;
	HBaseStorageSyncBuffer syncBuffer;
	
	TrxMan trxman;

	long trxCount = 0;
	long rowCount = 0;

	long putRowCount = 0;
	long deleteRowCount = 0;
	long indexRowCount = 0;
	
	long lastClosedTrxId = 0;
	
	boolean paused = false;
	boolean shuttingdown = false;
	
	public void setPaused(boolean paused) {
		this.paused = paused;
	}
	
	public boolean isPaused() {
		return this.paused;
	}
	
	public void shutdown() {
		shuttingdown = true;
	}
	
	public boolean isShuttingdown() {
		return shuttingdown;
	}
	
	public HBaseStorageHandler(Humpback humpback, HBaseStorageService hbaseStorageService) {
		super();
		this.humpback = humpback;
		this.spaceman = humpback.getSpaceManager();
		this.trxman = humpback.getTrxMan();
		this.hbaseStorageService = hbaseStorageService;
		this.paused = false;
		
		int bufferSize = hbaseStorageService.getConfigBufferSize();
		long currentSP = this.hbaseStorageService.getCurrentSP();
		this.syncBuffer = new HBaseStorageSyncBuffer(hbaseStorageService, currentSP, bufferSize);
	}

	public void run() throws Exception {
		Gobbler gobbler = getGobbler();
		if (gobbler == null) {
			// gobbler clould be null in the initialization stage when humpback is not ready
			return;
		}

		if (this.paused) return;

		// start sync from last point
		
		long spStart = this.syncBuffer.getBufferedSP();
		long end = gobbler.getPersistencePointer().get();
		try {
			// log
			_log.trace("start hbase sync from {} to {}", spStart, end);
			lastClosedTrxId = this.humpback.getLastClosedTransactionId();
			gobbler.replay(spStart, end, false, this);		// do sp in [spStart, end)

			this.syncBuffer.flush();
			
			this.putRowCount = this.syncBuffer.getPutRowCount();
			this.indexRowCount = this.syncBuffer.getIndexRowCount();
			
			// ending
			_log.trace("{} rows are synchronized - Put={}, Delete={}, Index={}", 
					this.rowCount, this.putRowCount, this.deleteRowCount, this.indexRowCount);
			// this.rowCount = 0;
		}
		catch (HBaseDataErrorJumpException ex) {
			this.syncBuffer.flush();
			throw ex;
		}
		catch (HBaseSyncErrorJumpException ex) {
			this.syncBuffer.flush();
			throw ex;
		}
		catch (HBaseStopJumpException ex) {
			// do nothing
			_log.debug(ex.getMessage());
		}
		catch (JumpException je) {
			// got a waiting to close trxid here or paused, shutdown...
			this.syncBuffer.flush();
		}
	}
	
	private void checkTrxid(long trxid) {
		if (this.lastClosedTrxId != 0) {
			if (trxid < this.lastClosedTrxId) {
				throw new JumpException();
			}
		}
	}
	
	private void checkPaused() {
		if (this.paused) throw new HBaseStopJumpException("hbase paused");
	}
	
	private void checkShutdown() {
		if (this.shuttingdown) throw new HBaseStopJumpException("hbase shutting down...");
	}

	@Override
	public void all(LogEntry entry) throws Exception {
		checkShutdown();
		checkPaused();
	}

	@Override
	public void put(Gobbler.PutEntry entry) {
		long sp = entry.getSpacePointer();
		int length = entry.getSize();
		long pRow = entry.getRowPointer();
		long spRow = entry.getRowSpacePointer();

		// check if the row is OK to go
		long trxid = Row.getVersion(pRow);
		checkTrxid(trxid);
		
		long trxts = this.trxman.getTimestamp(trxid);
		if (trxts < 0 && trxts >= -10) {
			// Rolled back transaction or other, we'll return directly
			_log.trace("rolled back put @ {} length={} - skipped", sp, length);
			this.syncBuffer.setBufferedSP(sp);
			return;
		}
		else if (trxts < 0){
			throw new HBaseDataErrorJumpException("Invalid TrxTs value - " + trxts);
		}

		_log.trace("put @ {} length={}", sp, length);

		// Retrieve Row data
		long version = Row.getVersion(pRow);
		int tableId = Row.getTableId(pRow);

		// filter out system table
		if (tableId != Integer.MAX_VALUE) {
			Row row = Row.fromSpacePointer(this.spaceman, spRow, version);
			try {
				this.syncBuffer.putRow(row, sp);				
			}
			catch (Exception ex) {
				// _log.error("failed to put @ {} trxid={}", sp, trxid, ex);
				String errorMsg = String.format("failed to put @ %1$d trxid=%2$d", sp, trxid);
				throw new HBaseSyncErrorJumpException(errorMsg, ex);
			}
			/*
			try {
				// Save to hbase table
				this.hbaseStorageService.put(row);
			}
			catch (Exception ex) {
				_log.error("error", ex);
				_log.info("faield to put @ {} trxid={} :{}", sp, trxid, ex.getMessage());
	    		// sleep for a while
	    		try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					
				}				
	    	}
			*/
			this.rowCount++;
		}
		
		// remember space pointer of last written 
		this.syncBuffer.setBufferedSP(sp);
	}

	@Override
	public void delete(DeleteEntry entry) throws Exception {
		int tableid = entry.getTableId();
		long sp = entry.getSpacePointer();
		long trxid = entry.getTrxid();
		long pkey = entry.getKeyAddress();

		checkTrxid(trxid);

		long trxts = this.trxman.getTimestamp(trxid);
		if (trxts < 0 && trxts >= -10) {
			// Rolled back transaction or other, we'll return directly
			_log.trace("rolled back delete @ {} tableId={} trxid={} - skipped", sp, tableid, trxid);
			this.syncBuffer.setBufferedSP(sp);
			return;
		}
		else if (trxts < 0){
			throw new HBaseDataErrorJumpException("Invalid TrxTs value - " + trxts);
		}

		// flushPutRows();
		
		// filter out sysmeta table
		if (tableid != Integer.MAX_VALUE) {
			
			// Check whether the delete row exists in buffer
			// If so, we can't delete it now
			if (this.syncBuffer.rowExists(tableid, pkey)) {
				this.syncBuffer.flush();
				// throw new JumpException();
			}
			
			_log.trace("delete @ {} tableId={} trxid={}", sp, tableid, trxid);
			
			// Delete from hbase table 
			this.hbaseStorageService.delete(tableid, pkey, trxid, sp);

			this.deleteRowCount++;
			this.rowCount++;
		}

		// remember space pointer of last written 
		this.syncBuffer.setBufferedSP(sp);
	}

	@Override
	public void commit(CommitEntry entry) {
		long sp = entry.getSpacePointer();
		long trxid = entry.getTrxid();
		long trxts = entry.getVersion();
		
		checkTrxid(trxid);

		_log.trace("commit @ {} trxid={} trxts={}", sp, trxid, trxts);
		
		// do nothing

		// remember space pointer of last written 
		this.syncBuffer.setBufferedSP(sp);

        this.trxCount++;
	}

	@Override
	public void rollback(RollbackEntry entry) {
		long sp = entry.getSpacePointer();
		long trxid = entry.getTrxid();
		
		checkTrxid(trxid);

		_log.trace("rollback @ {} trxid={}", sp, trxid);
		
		// do nothing for now
		
		// remember space pointer of last written 
		this.syncBuffer.setBufferedSP(sp);
		
	    this.trxCount++;
	}
	
	private Gobbler getGobbler() {
		return this.humpback.getGobbler();
	}
	
	@Override
	public void index(IndexEntry entry) {
		long sp = entry.getSpacePointer();
		int tableid = entry.getTableId();
		long trxid = entry.getTrxid();
		
		checkTrxid(trxid);

		long trxts = this.trxman.getTimestamp(trxid);
		if (trxts < 0 && trxts >= -10) {
			// Rolled back transaction or other, we'll return directly
			_log.trace("rolled back index @ {} tableId={} trxid={} - skipped", sp, tableid, trxid);
			return;
		}
		else if (trxts < 0){
			throw new HBaseDataErrorJumpException("Invalid TrxTs value - " + trxts);
		}
		
		_log.trace("set index @ {} trxid={}", sp, trxid);
		
		// filter out system table
		if (tableid != Integer.MAX_VALUE) {
			long rowKey = entry.getRowKeyAddress();
			long indexKey = entry.getIndexKeyAddress();

			try {
				// Save to hbase table
				this.syncBuffer.putIndex(tableid, trxts, rowKey, indexKey, sp);
			}
			catch (Exception ex) {
				// _log.error("failed to put @ {} trxid={}", sp, trxid, ex);
				String errorMsg = String.format("failed to put index @ %1$d trxid=%2$d", sp, trxid);
				throw new HBaseSyncErrorJumpException(errorMsg, ex);
			}

			this.rowCount++;
		}

		// remember space pointer of last written 
		this.syncBuffer.setBufferedSP(sp);
	}

	@Override
	public void message(MessageEntry entry) {
		long sp = entry.getSpacePointer();
		
		this.syncBuffer.setBufferedSP(sp);
	}

	@Override
	public void transactionWindow(TransactionWindowEntry entry) throws Exception {
		this.syncBuffer.startTrxId = entry.getTrxid();
		long sp = entry.getSpacePointer();
		this.syncBuffer.setBufferedSP(sp);
	}	
}
