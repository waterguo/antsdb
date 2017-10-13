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

import com.antsdb.saltedfish.nosql.Gobbler.CommitEntry;
import com.antsdb.saltedfish.nosql.Gobbler.DeleteEntry;
import com.antsdb.saltedfish.nosql.Gobbler.IndexEntry;
import com.antsdb.saltedfish.nosql.Gobbler.InsertEntry;
import com.antsdb.saltedfish.nosql.Gobbler.LogEntry;
import com.antsdb.saltedfish.nosql.Gobbler.MessageEntry;
import com.antsdb.saltedfish.nosql.Gobbler.PutEntry;
import com.antsdb.saltedfish.nosql.Gobbler.RowUpdateEntry;
import com.antsdb.saltedfish.nosql.Gobbler.RollbackEntry;
import com.antsdb.saltedfish.nosql.Gobbler.TransactionWindowEntry;
import com.antsdb.saltedfish.nosql.Gobbler.UpdateEntry;
import com.antsdb.saltedfish.util.JumpException;

/**
 * filter out rolled back data
 * 
 * @author *-xguo0<@
 */
public class TransactionReplayer implements ReplayHandler {
	private ReplayHandler downstream;
	private long sp;
	private TrxMan trxman;
	
	public TransactionReplayer(TrxMan trxman, ReplayHandler downstream) {
		this.trxman = trxman;
		this.downstream = downstream;
	}
	
	@Override
    public void insert(InsertEntry entry) throws Exception {
        if (isTrxRolledBack(entry)) {
            return;
        }
        this.downstream.insert(entry);
    }

    @Override
    public void update(UpdateEntry entry) throws Exception {
        if (isTrxRolledBack(entry)) {
            return;
        }
        this.downstream.update(entry);
    }

    @Override
	public void put(PutEntry entry) throws Exception {
		if (isTrxRolledBack(entry)) {
			return;
		}
		this.downstream.put(entry);
	}

	@Override
	public void index(IndexEntry entry) throws Exception {
		if (isTrxRolledBack(entry)) {
			return;
		}
		this.downstream.index(entry);
	}

	@Override
	public void delete(DeleteEntry entry) throws Exception {
		if (isTrxRolledBack(entry)) {
			return;
		}
		this.downstream.delete(entry);
	}

	@Override
	public void all(LogEntry entry) throws Exception {
		this.downstream.all(entry);
	}

	@Override
	public void commit(CommitEntry entry) throws Exception {
		this.downstream.commit(entry);
	}

	@Override
	public void rollback(RollbackEntry entry) throws Exception {
		this.downstream.rollback(entry);
	}

	@Override
	public void message(MessageEntry entry) throws Exception {
		this.downstream.message(entry);
	}

	@Override
	public void transactionWindow(TransactionWindowEntry entry) throws Exception {
		this.downstream.transactionWindow(entry);
	}

	private boolean isTrxRolledBack(DeleteEntry entry) {
		long trxid = entry.getTrxid();
		return isTrxRolledBack(trxid);
	}

	private boolean isTrxRolledBack(IndexEntry entry) {
		long trxid = entry.getTrxid();
		if (trxid == 0) {
		    throw new IllegalArgumentException("unexpected trxid=0");
		}
		return isTrxRolledBack(trxid);
	}

	private boolean isTrxRolledBack(RowUpdateEntry entry) {
		long trxid = entry.getTrxId();
		return isTrxRolledBack(trxid);
	}

	private boolean isTrxRolledBack(long trxid) {
		long version = trxman.getTimestamp(trxid);
		if (version > 0) {
			return false;
		}
		if (version == TrxMan.MARK_ROLLED_BACK) {
			return true;
		}
		throw new InvalidTransactionIdException(trxid);
	}

	public static long replay(Humpback humpback, long spStart, boolean inclusive, ReplayHandler handler) 
	throws Exception {
		Gobbler gobbler = humpback.getGobbler();
		if (gobbler == null) {
			// gobbler could be null in the initialization stage when humpback is not ready
			return spStart;
		}
		TransactionReplayer replayer = new TransactionReplayer(humpback.getTrxMan(), handler);
		replayer.sp = spStart;
		try {
			return humpback.getGobbler().replay(spStart, inclusive, replayer);
		}
		catch (JumpException x) {
			return replayer.sp;
		}
	}
}
