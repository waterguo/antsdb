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
package com.antsdb.saltedfish.nosql;

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.Bytes;
import com.antsdb.saltedfish.nosql.Gobbler.CommitEntry;
import com.antsdb.saltedfish.nosql.Gobbler.DeleteEntry;
import com.antsdb.saltedfish.nosql.Gobbler.IndexEntry;
import com.antsdb.saltedfish.nosql.Gobbler.RollbackEntry;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * recover the database from corruption
 *  
 * @author wgu0
 */
class Recoverer extends ReplayHandler {
	static Logger _log = UberUtil.getThisLogger();
	
	Humpback humpback;
	Gobbler gobbler;
	long trxCount = 0;
	long rowCount = 0;
	TrxMan trxman;
	
	public Recoverer(Humpback humpback, Gobbler gobbler) {
		super();
		this.humpback = humpback;
		this.gobbler = gobbler;
		this.trxman = humpback.getTrxMan();
	}
	
	public void run() throws Exception {
    	// find out the start point of recovery
    	
    	long start = Long.MAX_VALUE;
    	boolean inclusive = false;
    	for (GTable i:this.humpback.tableById.values()) {
    		long sp = i.getEndRowSpacePointer();
    		if (sp == 0) {
    			// 0 means the table is empty
    			continue;
    		}
    		start = Math.min(start, sp);
    	}
    	if (start == Long.MAX_VALUE) {
    		start = this.gobbler.getStartSp() + Gobbler.ENTRY_HEADER_SIZE;
    		inclusive = true;
    	}
    	
    	// start recovering from the start given point
    	
    	_log.info("start recovering from {} to {}", start, this.humpback.spaceman.getAllocationPointer());
    	if (start != Long.MAX_VALUE) {
			this.gobbler.replayFromRowPointer(start, this, inclusive);
    	}
    	
    	// ending
    	
    	_log.info("{} rows have been recovered", this.rowCount);
    	_log.info("{} transactions have been recovered", this.trxCount);
	}
	
	@Override
	public void put(Gobbler.PutEntry entry) {
		long pRow = entry.getRowPointer();
		long sp = entry.getSpacePointer();
		long spRow = entry.getRowSpacePointer();
		long version = Row.getVersion(pRow);
		int tableId = Row.getTableId(pRow);
		GTable table = this.humpback.getTable(tableId);
		long pKey = Row.getKeyAddress(pRow);
		if (_log.isTraceEnabled()) {
			_log.trace("put @ {} tableId={} version={} key={}", sp, tableId, version, Bytes.toString(pKey));
		}
		if (table == null) {
			_log.warn("unable to recover row @ {}. table {} not found", sp, tableId);
			return;
		}
		if (sp <= table.getEndRowSpacePointer()) {
			// space pointer i ahead of end row space pointer means the operation has already applied
			_log.trace("put @ {} is ignored", sp);
			return;
		}
		HumpbackError error = table.putNoLogging(version, pKey, spRow);
		if (HumpbackError.SUCCESS != error) {
			_log.warn("unable to recover row @ {} due to {}", sp, error);
			return;
		}
		
		// if system table is touched, synchronize with file system
		
        if (tableId == Integer.MAX_VALUE) {
        	try {
				humpback.sync();
			}
			catch (ClassNotFoundException e) {
				_log.warn("unable to recreate table {} @ {}.", tableId, sp, e);
				return;
			}
        }
		this.rowCount++;
	}

	@Override
	public void delete(DeleteEntry entry) {
		int tableId = entry.getTableId();
		long sp = entry.getSpacePointer();
		long trxid = entry.getTrxid();
		long pKey = entry.getKeyAddress();
		
		if (_log.isTraceEnabled()) {
			_log.trace("delete @ {} tableId={} version={} key={}", sp, tableId, trxid, Bytes.toString(pKey));
		}
		GTable table = this.humpback.getTable(tableId);
		if (table == null) {
			_log.warn("unable to recover row @ {}. table {} not found", sp, tableId);
			return;
		}
		if (sp <= table.getEndRowSpacePointer()) {
			// space pointer i ahead of end row space pointer means the operation has already applied
			_log.trace("delete @ {} is ignored", sp);
			return;
		}
		HumpbackError error = table.deleteNoLogging(trxid, pKey, sp + Gobbler.ENTRY_HEADER_SIZE, 1000);
		if (HumpbackError.SUCCESS != error) {
			_log.warn("unable to recover row @ {} due to {}", sp, error);
			return;
		}
		this.rowCount++;
	}

	@Override
	public void commit(CommitEntry entry) {
		long sp = entry.getSpacePointer();
		long trxid = entry.getTrxid();
		long trxts = entry.getVersion();
		
		_log.trace("commit @ {} trxid={} trxts={}", sp, trxid, trxts);
    	this.trxman.commit(trxid, trxts);
        this.trxCount++;
	}

	@Override
	public void rollback(RollbackEntry entry) {
		long sp = entry.getSpacePointer();
		long trxid = entry.getTrxid();

		_log.trace("commit @ {} trxid={}", sp, trxid);
		this.trxman.rollback(trxid);
        this.trxCount++;
	}

	@Override
	public void index(IndexEntry entry) {
		if (_log.isTraceEnabled()) {
			_log.trace("index @ {} tableId={} version={} key={}", 
					   entry.getSpacePointer(), 
					   entry.getTableId(), 
					   entry.getTrxid(), 
					   Bytes.toString(entry.getIndexKeyAddress()));
		}
		GTable table = this.humpback.getTable(entry.getTableId());
		if (table == null) {
			_log.warn("unable to recover index @ {}. table {} not found", entry.getSpacePointer(), entry.getTableId());
			return;
		}
		if (entry.getSpacePointer() <= table.getEndRowSpacePointer()) {
			// space pointer i ahead of end row space pointer means the operation has already applied
			_log.trace("index @ {} is ignored", entry.getSpacePointer());
			return;
		}
		HumpbackError error = table.insertIndex_nologging(entry.getTrxid(), 
				                                entry.getIndexKeyAddress(), 
				                                entry.getRowKeyAddress(),
				                                entry.getSpacePointer(),
				                                0);
		if (HumpbackError.SUCCESS != error) {
			_log.warn("unable to recover index @ {} due to {}", entry.getSpacePointer(), error);
			return;
		}
	}

}
