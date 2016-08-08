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
package com.antsdb.saltedfish.sql.vdm;

import java.util.concurrent.atomic.AtomicIntegerArray;

import com.antsdb.saltedfish.sql.RecycleThread;
import com.antsdb.saltedfish.util.UberTime;

public class Measure {
    static final int METRICS_SECONDS = 60;
    static final int METRICS_SLOT_NSECONDS = 5;
    static final int MAX_DISTANCE = METRICS_SECONDS / METRICS_SLOT_NSECONDS + 1;
    /* must need additional 2 because of recycling
     * current slot + next slot in case of at the end of current slot
     */
    static final int METRICS_NSLOTS = 
            (int)((METRICS_SECONDS + RecycleThread.RECYLE_INTERVAL_SECONDS) / METRICS_SLOT_NSECONDS + 2);
    enum Field {
        EPOCH,
        COUNT,
        ELAPSE,
        MIN,
        MAX,
        NFIELDS
    }
    
    volatile boolean enabled = true;
    AtomicIntegerArray metrics = new AtomicIntegerArray(METRICS_NSLOTS * Field.NFIELDS.ordinal());
    
    public Measure() {
        recycle();
    }

    int getEpoch(long nanotime) {
        long epoch = nanotime / 1000000000 / METRICS_SLOT_NSECONDS;
        return (int)epoch;
    }
    
    int getSlot(long epoch) {
        int slot = (int)(epoch % METRICS_NSLOTS);
        return slot;
    }
    
    public long getTime() {
        long time = (this.enabled) ? UberTime.getNanoTime() : 0;
        return time;
    }
    
    void measure(long start) {
        if (start == 0) {
            return;
        }
        
        long now = getTime();
        int epoch = getEpoch(now);
        int slot = getSlot(epoch);
        if (getEpoch(slot) != epoch) {
            // fault tolerance if these two values don't match for whatever reason, don't proceed
            return;
        }
        int elapse = (int)(now - start);
        
        // record metrics 
        
        increaseCount(slot);
        setMax(slot, elapse);
        setMin(slot, elapse);
        addElapse(slot, elapse);

        // don't measure anything faster than 20 microsecond. time retrieval is too expensive
        
        if (elapse < 10000) {
            // this.enabled = false;
        }
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    private void setMax(int slot, int elapse) {
        int idx = slot * Field.NFIELDS.ordinal() + Field.MAX.ordinal();
        for (;;) {
            int n = this.metrics.get(idx);
            if (elapse <= n ) {
                break;
            }
            if (this.metrics.compareAndSet(idx, n, elapse)) {
                break;
            }
        }
    }

    private void setMin(int slot, int elapse) {
        int idx = slot * Field.NFIELDS.ordinal() + Field.MIN.ordinal();
        for (;;) {
            int n = this.metrics.get(idx);
            if (elapse >= n ) {
                break;
            }
            if (this.metrics.compareAndSet(idx, n, elapse)) {
                break;
            }
        }
    }

    private int getElapse(int slot) {
        int idx = slot * Field.NFIELDS.ordinal() + Field.ELAPSE.ordinal();
        return this.metrics.get(idx);
    }
    
    private void addElapse(int slot, int elapse) {
        int idx = slot * Field.NFIELDS.ordinal() + Field.ELAPSE.ordinal();
        for (;;) {
            int n = this.metrics.get(idx);
            if (this.metrics.compareAndSet(idx, n, n + elapse)) {
                break;
            }
        }
    }

    private int getCount(int slot) {
        int idx = slot * Field.NFIELDS.ordinal() + Field.COUNT.ordinal();
        int val = this.metrics.get(idx);
        return val;
    }
    
    private int getMin(int slot) {
        int idx = slot * Field.NFIELDS.ordinal() + Field.MIN.ordinal();
        int val = this.metrics.get(idx);
        return val;
    }
    
    private int getMax(int slot) {
        int idx = slot * Field.NFIELDS.ordinal() + Field.MAX.ordinal();
        int val = this.metrics.get(idx);
        return val;
    }
    
    private void increaseCount(int slot) {
        int idx = slot * Field.NFIELDS.ordinal() + Field.COUNT.ordinal();
        this.metrics.incrementAndGet(idx);
    }

    private long getEpoch(int slot) {
        long val = this.metrics.get(slot * Field.NFIELDS.ordinal() + Field.EPOCH.ordinal());
        return val;
    }

    public void recycle() {
        long now = UberTime.getNanoTime();
        int epoch = getEpoch(now);
        int epochSlot = getSlot(epoch);
        for (int i=0; i<METRICS_NSLOTS; i++) {
            long slotEpoch = getEpoch(i);
            if ((epoch - slotEpoch) > MAX_DISTANCE) {
                int newEpoch = (i + METRICS_NSLOTS - epochSlot) % METRICS_NSLOTS + epoch;
                this.metrics.set(i * Field.NFIELDS.ordinal() + Field.EPOCH.ordinal(), newEpoch);
                this.metrics.set(i * Field.NFIELDS.ordinal() + Field.COUNT.ordinal(), 0);
                this.metrics.set(i * Field.NFIELDS.ordinal() + Field.ELAPSE.ordinal(), 0);
                this.metrics.set(i * Field.NFIELDS.ordinal() + Field.MIN.ordinal(), Integer.MAX_VALUE);
                this.metrics.set(i * Field.NFIELDS.ordinal() + Field.MAX.ordinal(), 0);
            }
        }
    }

    public int getCount(long now) {
        int epoch = getEpoch(now);
        int count = 0;
        for (int i=0; i<METRICS_NSLOTS; i++) {
            long slotEpoch = getEpoch(i);
            long distance = epoch - slotEpoch;
            if ((distance < MAX_DISTANCE) && (distance > 0)) {
                count += this.getCount(i);
            }
        }
        return count;
    }

    public int getMinLatency(long now) {
        int epoch = getEpoch(now);
        int min = Integer.MAX_VALUE;
        for (int i=0; i<METRICS_NSLOTS; i++) {
            long slotEpoch = getEpoch(i);
            long distance = epoch - slotEpoch;
            if ((distance < MAX_DISTANCE) && (distance > 0)) {
                min = Math.min(min, getMin(i));
            }
        }
        return min;
    }

    public int getMaxlatency(long now) {
        int epoch = getEpoch(now);
        int max = 0;
        for (int i=0; i<METRICS_NSLOTS; i++) {
            long slotEpoch = getEpoch(i);
            long distance = epoch - slotEpoch;
            if ((distance < MAX_DISTANCE) && (distance > 0)) {
                max = Math.max(max, getMax(i));
            }
        }
        return max;
    }

    public int getTotalLatency(long now) {
        int epoch = getEpoch(now);
        int total = 0;
        for (int i=0; i<METRICS_NSLOTS; i++) {
            long slotEpoch = getEpoch(i);
            long distance = epoch - slotEpoch;
            if ((distance < MAX_DISTANCE) && (distance > 0)) {
                total += getElapse(i);
            }
        }
        return total;
    }

    public boolean isEnabled() {
        return this.enabled;
    }
}
