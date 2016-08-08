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

import java.io.File;
import java.io.IOException;
import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.Gobbler;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.util.UberUtil;

public class HBaseStorageServerThread extends Thread {
    static final Object EOF_MARK = "EOF";
    static Logger _log = UberUtil.getThisLogger();

	Gobbler gobbler = null;
    Humpback humpback = null;
    HBaseStorageService hbaseStorageService = null;
	HBaseStorageHandler hbaseHandler;
    File home;
    
    public HBaseStorageServerThread(HBaseStorageService service, Humpback humpback) throws IOException {
        setName(getClass().getSimpleName());
        setDaemon(true);
        this.hbaseStorageService = service;
        this.humpback = humpback;
        gobbler = humpback.getGobbler();
        hbaseHandler = new HBaseStorageHandler(this.humpback, this.hbaseStorageService);
    }
    
    @Override
    public void run() {
        try {
    		_log.info("{} started...", getName());
            mainloop();
        }
        catch (Exception x) {
            _log.error("failures from HBaseStorageServer thread.", x);
        }
    }
    
    public void shutdown() {
        this.hbaseHandler.shutdown();

        // wait the thread to die        
        try {
    		_log.info("{} is shutting down ...", getName());
            join(5000);
    		_log.info("{} is shut down", getName());
        }
        catch (InterruptedException ignored) {
        	_log.warn("{} is forced shut down", getName());
        }        
    }
    
    public void setPaused(boolean paused) {
    	this.hbaseHandler.setPaused(paused);
    }

    private void mainloop() {
    	boolean panic = false; 
    	
    	// set current SP

        for (;;) { 

        	// shutdown ?
        	
            if (this.hbaseHandler.isShuttingdown()) {
                break;
            }

            if (!this.hbaseHandler.isPaused()) {
	            // replay
	            
	        	try {
	        		long currentSp = this.hbaseStorageService.getCurrentSP();
	        		replay();
	        		if (panic) {
	        			_log.info("{} is resumed from panic", getName());
	            		panic = false;
	        		}
	        		if (currentSp != this.hbaseStorageService.getCurrentSP()) {
	        			// there is progress, replay again
	        			this.hbaseStorageService.cp.updateHBase();
	        			continue;
	        		}
	        	}
	        	catch (HBaseDataErrorJumpException x) {
	        		_log.error("Critical data error detected... hbase will shutdown now", x);
	        		this.shutdown();
	        	}
	        	catch (Exception e) {
	        		if (!panic) {
	        			// panic control to prevent log from being flooded
	        			panic = true;
	        			_log.error("failure", e);
	        		}
		        }
            }
            else {
            	try
            	{
            		Thread.sleep(10000);            	
    			} 
        		catch (InterruptedException e) {
    			}
            }
        	
        	// wait a little if there is error or there is no progress
        	
    		try {
				Thread.sleep(1000);
			} 
    		catch (InterruptedException e) {
			}
        }
    }

	private void replay() throws Exception {
		hbaseHandler.run();
	}    
}
