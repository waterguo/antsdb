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
 * 
 * @author *-xguo0<@
 */
public class RecentCounter {
    long total;
    long then;
    double counter;
    long counterBySecond[];
    int seconds;
    
    public RecentCounter(int seconds) {
        this.counterBySecond = new long[seconds];
        this.seconds = seconds;
    }
    
    public void count(long n) {
        long now = UberTime.getTime() / 1000;
        if (now > this.then) {
            for (int i=this.seconds-1; i>=0; i--) {
                long newSlotTime = now - i;
                long idx = this.then - newSlotTime;
                if (idx >= 0) {
                    this.counterBySecond[i] = this.counterBySecond[(int)idx];
                }
                else {
                    this.counterBySecond[i] = 0;
                }
            }
        }
        this.counterBySecond[0] = this.counterBySecond[0] + n;
        this.then = now;
        this.total++;
    }
    
    public long getCount() {
        long temp = this.then;
        long now = UberTime.getTime() / 1000;
        long result = 0;
        for (int i=0; i<this.seconds-1; i++) {
            long slotTime = temp - i;
            if (now - slotTime <= this.seconds) {
                result += this.counterBySecond[i];
            }
        }
        return result;
    }
    
    public long getTotal() {
        return this.total;
    }
}
