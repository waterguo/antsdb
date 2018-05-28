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
package com.antsdb.saltedfish.nosql;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 
 * @author wgu0
 */
public class RowLockMonitor {
	volatile boolean isEnabled = false;
    Map<Long, RowLockInfo> locks = new ConcurrentHashMap<>();
	
    public static class RowLockInfo {
    	public long requestTrxId;
    	public long lockTrxId;
    	public File file;
    	public long pos;
    }
    
	public synchronized void start() {
		this.isEnabled = true;
		this.locks.clear();
	}
	
	public synchronized Map<Long, RowLockInfo> end() {
		this.isEnabled = false;
		return this.locks;
	}
	
	public void register(long requestTrxid, long lockTrxid, long pKey, File file, long pos) {
		if (!this.isEnabled) {
			return;
		}
		RowLockInfo info = this.locks.get(requestTrxid);
		if (info != null) {
			if (info.lockTrxId == lockTrxid) {
				return;
			}
		}
		else {
			info = new RowLockInfo();
			this.locks.put(requestTrxid, info);
		}
		info.requestTrxId = requestTrxid;
		info.lockTrxId = lockTrxid;
		info.file = file;
		info.pos = pos;
	}
}
