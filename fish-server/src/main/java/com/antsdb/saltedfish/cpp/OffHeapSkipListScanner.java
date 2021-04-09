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
package com.antsdb.saltedfish.cpp;

import com.antsdb.saltedfish.nosql.ScanOptions;

/**
 * scan for elements stored in OffHeapSkipList
 * 
 * @author *-xguo0<@
 */
public final class OffHeapSkipListScanner {
    private OffHeapSkipList owner;
    private long pStartNode;
    private long pEndNode;
    private long pNextNode;
    private long pKey;
    
    public OffHeapSkipListScanner(OffHeapSkipList owner) {
        this.owner = owner;
    }
    
    public void reset(long pKeyStart, long pKeyEnd, long options) {
        this.pNextNode = 0;
        this.pStartNode = ScanOptions.includeStart(options) ? 
                this.owner.findNode(pKeyStart, OffHeapSkipList.OP_GE) :
                this.owner.findNode(pKeyStart, OffHeapSkipList.OP_GT);
        this.pEndNode = ScanOptions.includeEnd(options) ? 
                this.owner.findNode(pKeyEnd, OffHeapSkipList.OP_LE) :
                this.owner.findNode(pKeyEnd, OffHeapSkipList.OP_LT);
        if (this.pEndNode ==0 || this.pStartNode ==0) return;
        long pKeyScanStart = OffHeapSkipList.Node.getKeyPointer(this.pStartNode);
        long pKeyScanEnd = OffHeapSkipList.Node.getKeyPointer(this.pEndNode);
        if (this.owner.compare(pKeyScanStart, pKeyScanEnd) > 0) return;
        this.pNextNode = this.pStartNode;
    }
    
    /**
     * 0 if there no more
     * 
     * @return pointer to the 64 bits value
     */
    public long next() {
        if (pNextNode != 0) {
            long result = OffHeapSkipList.Node.getValuePointer(this.pNextNode);
            this.pKey = OffHeapSkipList.Node.getKeyPointer(this.pNextNode);
            if (this.pNextNode == this.pEndNode) {
                this.pNextNode = 0;
            }
            else {
                this.pNextNode = OffHeapSkipList.Node.getNext(this.pNextNode);
            }
            return result;
        }
        else {
            return 0;
        }
    }
    
    public long getKeyPointer() {
        return this.pKey;
    }
}
