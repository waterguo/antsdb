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
    
	default public long getVersion() {
		return Row.getVersion(getRowPointer());
	}
	
	public abstract long getRowScanned();
    
	public abstract void rewind();
    
	default Row getRow() {
    	return Row.fromMemoryPointer(getRowPointer(), getVersion());
    }
    
    public abstract void close();

	public abstract boolean isRow();
}
