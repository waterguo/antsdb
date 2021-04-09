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
package com.antsdb.saltedfish.sql.vdm;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.KeyBytes;

/**
 * 
 * @author *-xguo0<@
 */
public abstract class RecordBase {
    long addr;
    
    public RecordBase(long addr) {
        this.addr = addr;
    }
    
    public Object getValue(int field) {
        long pValue = get(field);
        return FishObject.get(null, pValue);
    }
    
    public byte[] getKeyBytes() {
        long pKey = getKey();
        return pKey != 0 ? KeyBytes.get(pKey) : null;
    }
    
    public abstract long get(int field);
    
    public abstract long getKey();
}
