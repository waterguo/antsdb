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
package com.antsdb.saltedfish.nosql;

/**
 * 
 * @author *-xguo0<@
 */
public final class IndexLine {
    private long indexKey;
    private long rowKey;
    private byte misc;

    public IndexLine(long pLine) {
        Gobbler.IndexEntry entry = new Gobbler.IndexEntry(0, pLine);
        this.indexKey = entry.getIndexKeyAddress();
        this.rowKey = entry.getRowKeyAddress();
        this.misc = entry.getMisc();
    }
    
    public static IndexLine from(long pLine) {
        if (pLine == 0) {
            return null;
        }
        return new IndexLine(pLine);
    }
    
    public long getKey() {
        return this.indexKey;
    }
    
    public long getRowKey() {
        return this.rowKey;
    }
    
    public byte getMisc() {
        return this.misc;
    }
}
