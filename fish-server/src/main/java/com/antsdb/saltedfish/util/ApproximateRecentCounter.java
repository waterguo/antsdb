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
package com.antsdb.saltedfish.util;

/**
 * counter for the last n seconds
 * 
 * @author *-xguo0<@
 */
public class ApproximateRecentCounter {
    long total;
    long then;
    double counter;
    long ms;
    
    public ApproximateRecentCounter(int seconds) {
        this.ms = seconds * 1000;
    }
    
    public void count(long n) {
        total += n;
        this.counter = getCount() + n;
        this.then = UberTime.getTime();
    }
    
    public double getCount() {
        double now = UberTime.getTime();
        double ratio = 1 - (now - then) / this.ms; 
        double result = (ratio > 0) ? this.counter * ratio : 0;
        return result;
    }
    
    public long getTotal() {
        return this.total;
    }
}
