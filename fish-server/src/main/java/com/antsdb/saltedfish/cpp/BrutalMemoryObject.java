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

/**
 * parent of all direct memory based object
 * 
 * @author *-xguo0<@
 */
public abstract class BrutalMemoryObject {
    protected long addr;
    
    public BrutalMemoryObject(long addr) {
        this.addr = addr;
    }
    
    public final long getAddress() {
        return this.addr;
    }
    
    /** number of bytes taken by this object */
    public abstract int getByteSize();
    
    /** get the byte used identify the object in memory. the format is defined in Value class */
    public abstract int getFormat();
}
