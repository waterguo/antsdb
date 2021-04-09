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

/**
 * Knob is a class to pause a thread
 * 
 * @author *-xguo0<@
 */
public final class Knob {
    private String name;
    private Scheduler master;
    private boolean pause = false;
    private boolean sleep = false;
    private Thread thread;
    private int priority;
    private String progress;

    public Knob(Scheduler master, String name, int priority) {
        this.name = name;
        this.master = master;
        this.priority = priority;
    }
    
    public void start() {
        this.pause  = false;
    }
    
    public void pause() {
        this.pause = true;
    }
    
    /**
     * check the schedule for potential pause
     */
    public void pong() throws InterruptedException {
        while (this.pause || (this.priority >= 1 && this.master.getUserLoad() >= 25)) {
            // let the user thread do their shit with highest priority
            try {
                this.sleep = true;
                Thread.sleep(1000);
            }
            finally {
                this.sleep = false;
            }
        }
    }

    public boolean isPaused() {
        return this.pause;
    }

    public String getName() {
        return this.name;
    }
    
    public boolean isSleeping() {
        return this.sleep;
    }

    public Thread getThread() {
        return thread;
    }

    public void setThread(Thread thread) {
        this.thread = thread;
    }

    public int getPriority() {
        return this.priority;
    }

    public String getProgress() {
        return this.progress;
    }

    public void setProgress(String progress) {
        this.progress = progress;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }
}
