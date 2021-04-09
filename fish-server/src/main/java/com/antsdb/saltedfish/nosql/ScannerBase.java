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

import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.cpp.SkipListScanner;
import com.antsdb.saltedfish.nosql.MemTablet.ListNode;

/**
 * 
 * @author *-xguo0<@
 */
abstract class ScannerBase extends ScanResult {
    protected SkipListScanner upstream;
    protected MemTablet tablet;
    protected long base;
    protected TrxMan trxman;
    protected int oNext;
    private boolean eof = false;

    abstract int findNext();

    ScannerBase(MemTablet tablet, SkipListScanner upstram) {
        this.tablet = tablet;
        this.base = tablet.base;
        this.trxman = tablet.trxman;
        this.upstream = upstram;
    }
    
    @Override
    public boolean next() {
        if (eof) {
            return false;
        }
        this.oNext = findNext();
        if (this.oNext == 0) {
            this.eof = true;
            return false;
        }
        return true;
    }
    
    @Override
    public long getVersion() {
        if (this.eof) {
            return 0;
        }
        ListNode node = new ListNode(base, oNext);
        long result = node.getVersion();
        if (result < -10) {
            result = this.trxman.getTimestamp(result);
        }
        return result;
    }
    
    @Override
    public long getRowPointer() {
        if (this.eof) {
            return 0;
        }
        ListNode node = new ListNode(base, oNext);
        if (node.isDeleted()) {
            return 1;
        }
        long sp = node.getSpacePointer();
        return this.tablet.sm.toMemory(sp) + RowUpdateEntry2.getHeaderSize();
    }

    
    @Override
    public long getLogPointer() {
        if (this.eof) {
            return 0;
        }
        ListNode node = new ListNode(base, oNext);
        long sp = node.getSpacePointer();
        return sp;
    }
    
    @Override
    public long getKeyPointer() {
        if (this.eof) {
            return 0;
        }
        return this.upstream.getKeyPointer(); 
    }
    
    @Override
    public long getIndexRowKeyPointer() {
        if (this.eof) {
            return 0;
        }
        return ListNode.getRowKeyAddress(base, oNext);
    }
    
    @Override
    public void rewind() {
        this.upstream.rewind();
        this.oNext = 0;
    }
    
    @Override
    public boolean eof() {
        if (this.eof) {
            return true;
        }
        return this.upstream.eof();
    }
    
    @Override
    public long getIndexSuffix() {
        if (this.eof) {
            return 0;
        }
        long pKey = getKeyPointer();
        if (pKey == 0) {
            return 0;
        }
        long suffix = KeyBytes.create(pKey).getSuffix();
        return suffix;
    }

    @Override
    public byte getMisc() {
        if (this.eof) {
            return 0;
        }
        ListNode node = new ListNode(base, oNext);
        return node.getMisc();
    }

    @Override
    public void close() {
        this.eof = true;
    }

    @Override
    public String toString() {
        return getLocation();
    }
    
    @Override
    public String getLocation() {
        if (this.eof) {
            return "eof";
        }
        String result = this.tablet.getRowLocation(this.oNext, getLogPointer());
        return result;
    }
}
