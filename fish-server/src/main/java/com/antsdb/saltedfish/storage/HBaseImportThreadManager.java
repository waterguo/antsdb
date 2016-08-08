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

import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.storage.HBaseUtilImporter.TableRows;
import com.antsdb.saltedfish.util.UberUtil;

public class HBaseImportThreadManager {
    static Logger _log = UberUtil.getThisLogger();
	
	HBaseImportThread[] threads = null;

	public HBaseImportThreadManager(int count, Connection hbaseConn, Humpback humpback, MetadataService metaService,
					boolean ignoreError, int columnsPerPut) {
		threads = new HBaseImportThread[count];
		for (int i=0; i<count; i++) {
			threads[i] = new HBaseImportThread(hbaseConn, humpback, metaService, ignoreError, columnsPerPut);
			threads[i].setName("HBaseImportThead " + (i+1));
		}
	    _log.info("{} import threads created.", count);
	}
	
	public void start() {
		for (HBaseImportThread t : threads) {
			t.start();
		}		
	}
	
	public void putRows(TableRows r) throws Exception {
		
		for (;;) {
			if (allThreadsExit()) {
				throw new Exception("************* All importing threads exit already *************\n");
			}
			
			for(HBaseImportThread t: threads) {
				if (!t.isTerminated()) {
					if (t.putBuffer(r)) {
						return;
					}
				}
			}
			
			try {
				Thread.sleep(100);					
			}
			catch (Exception ex) {
			}
		}
	}

	boolean allThreadsExit() {
		for (HBaseImportThread t : threads) {
			if (!t.isTerminated()) {
				return false;
			}
		}
		return true;
	}
	
	public void shutdown() {
		
		for (HBaseImportThread t: threads) {
			t.shutdown();
		}
		
		try {			
			for (HBaseImportThread t : threads) {
				t.join();
			}
		}
		catch(InterruptedException e) {
			
		}
	}
	
	public void waitForFinish() {
		for (HBaseImportThread t: threads) {
			t.waitingForFinish();
		}
		
		try {			
			for (HBaseImportThread t : threads) {
				t.join();
			}
		}
		catch(InterruptedException e) {
			
		}
	}
}
