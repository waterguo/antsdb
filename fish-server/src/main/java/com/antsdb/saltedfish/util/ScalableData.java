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
 * a concurrent scalable data structure
 * 
 * @author *-xguo0<@
 */
public abstract class ScalableData {
    private volatile long ticket = Long.MIN_VALUE;
    
    protected synchronized void resetTicket() {
        this.ticket = Long.MIN_VALUE;
    }
    
    /**
     * grow the current storage 
     * 
     * @param filled the storage object that is full 
     * @return
     */
    protected synchronized boolean grow(long requestTicket) {
        if (requestTicket <= this.ticket) {
            // caller should try again because a new storage object is created
            return false;
        }
        this.ticket = requestTicket;
        extend();
        return true;
    }
    
    /**
     * Overridden by subclass to extend the storage object. this method will be called in a single thread context
     */
    protected abstract void extend();
}
