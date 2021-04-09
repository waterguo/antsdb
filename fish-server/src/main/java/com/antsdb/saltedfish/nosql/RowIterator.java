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

import java.io.Closeable;
import java.util.NoSuchElementException;

public interface RowIterator extends Closeable {
    /**
     * Returns the virtual pointer to the actual row
     *
     * @return the next element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    public abstract boolean next();

    /**
     * pointer to the row key. only applicable to index entry
     * @return
     */
    public abstract long getRowKeyPointer();
    
    /**
     * index suffix. only applicable to non-unique index
     * 
     * @return
     */
    public abstract long getIndexSuffix();
    
    /**
     * if the iterator has reached eof
     * @return
     */
    public abstract boolean eof();
    
    /**
     * pointer to the row, only applicable to row entry
     * @return
     */
    public abstract long getRowPointer();
    
    /**
     * pointer to the key
     * 
     * @return
     */
    public abstract long getKeyPointer();
    
    /**
     * get the misc byte for the index entry
     * 
     * @return
     */
    public abstract byte getMisc();
    
    /**
     * get the location of the row 
     * 
     * @return
     */
    public abstract String getLocation();
    
    default public long getVersion() {
        long pRow = getRowPointer();
        if (pRow < 10) {
            throw new IllegalArgumentException();
        }
        return Row.getVersion(getRowPointer());
    }
    
    public abstract long getRowScanned();
    
    public abstract void rewind();
    
    default Row getRow() {
        return Row.fromMemoryPointer(getRowPointer(), getVersion());
    }
    
    public abstract void close();
    
    public default long getLogPointer() {
        return 0;
    }
}
