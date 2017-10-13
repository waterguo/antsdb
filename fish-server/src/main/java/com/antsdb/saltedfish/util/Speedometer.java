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
 * 
 * @author *-xguo0<@
 */
public class Speedometer {
    private int duration;
    private Sample[] samples;
    
    static class Sample {
        long timestamp;
        long value;
    }
    
    public Speedometer() {
        this(10);
    }
    
    /**
     * 
     * @param duration sample duration in seconds
     */
    public Speedometer(int duration) {
        this.duration = duration;
        this.samples = new Sample[duration];
        for (int i=0; i<this.samples.length; i++) {
            this.samples[i] = new Sample();
        }
    }
    
    public void sample(long mileage) {
        long now = UberTime.getTime() / 1000;
        int idx = (int)(now % this.duration);
        this.samples[idx].timestamp = now;
        this.samples[idx].value = mileage;
    }
    
    public long getSpeed() {
        long now = UberTime.getTime() / 1000;
        long minTimestamp = Long.MAX_VALUE;
        long minValue = Long.MAX_VALUE;
        long total = 0;
        long totalTimestamp = 0;
        int count = 0;
        for (Sample i:this.samples) {
            if ((now - i.timestamp) > this.duration) {
                continue;
            }
            if (i.timestamp < minTimestamp) {
                minTimestamp = i.timestamp;
                minValue = i.value;
            }
            total += i.value;
            totalTimestamp += i.timestamp;
            count++;
        }
        if (count <= 0) {
            return 0;
        }
        if (count <= 1) {
            // not enough samples
            return -1;
        }
        long result = (total - minValue * count) / (totalTimestamp - minTimestamp * count);
        return result;
    }
}
