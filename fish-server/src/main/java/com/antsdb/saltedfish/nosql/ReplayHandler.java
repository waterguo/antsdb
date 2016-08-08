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
import com.antsdb.saltedfish.nosql.Gobbler.LogEntry;
import com.antsdb.saltedfish.nosql.Gobbler.MessageEntry;
import com.antsdb.saltedfish.nosql.Gobbler.RollbackEntry;
import com.antsdb.saltedfish.nosql.Gobbler.TransactionWindowEntry;

/**
 * implemented by caller
 * 
 * @author wgu0
 */
public class ReplayHandler {
	public void all(LogEntry entry) throws Exception {
	}
	
	public void put(Gobbler.PutEntry entry) throws Exception {
	}
	
	public void index(Gobbler.IndexEntry entry) throws Exception {
	}
	
	public void delete(Gobbler.DeleteEntry entry) throws Exception {
	}
	
	public void commit(CommitEntry entry) throws Exception {
	}
	
	public void rollback(RollbackEntry entry) throws Exception {
	}
	
	public void message(MessageEntry entry) throws Exception {
	}

	public void transactionWindow(TransactionWindowEntry entry) throws Exception {
	}
}
