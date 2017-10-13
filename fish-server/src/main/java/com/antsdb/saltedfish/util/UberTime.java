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
package com.antsdb.saltedfish.util;

/**
 * very high performance time retrieval but suffers from measurement granularity. precision of the time is 2ms 
 * 
 * @author xinyi
 *
 */
public class UberTime extends Thread {
    static final int SLEEP_MS = 1;
    static volatile long _nanoTime;
    static volatile long _milliTime;
    
    static {
        UberTime._nanoTime = System.nanoTime();
        _milliTime = System.currentTimeMillis();
        new UberTime().start();
    }

    private UberTime() {
        setName("Uber Time Thread");
        setDaemon(true);
    }
    
    /**
     * get time in nanoseconds
     * 
     * @return
     */
    public final static long getNanoTime() {
        return UberTime._nanoTime;
    }
    
    /**
     * Equivalent to System.currentTimeMillis()
     * @return
     */
    public final static long getTime() {
    	return _milliTime;
    }
    
    /**
     * get the time uncertainty in ms
     * @return
     */
    public final static long getTimeUncertainty() {
        return SLEEP_MS * 100;
    }
    
    @Override
    public void run() {
        for (;;) {
            UberTime._nanoTime = System.nanoTime();
            _milliTime = System.currentTimeMillis();
            try {
                Thread.sleep(SLEEP_MS);
            }
            catch (InterruptedException e) {
            }
        }
    }
    
    /**
     * wait until time moves forward
     */
    public static void step() {
        long time = getTime();
        while (time == getTime()) {
        }
    }
}