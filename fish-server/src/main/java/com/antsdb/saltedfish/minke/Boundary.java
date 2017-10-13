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
package com.antsdb.saltedfish.minke;

import com.antsdb.saltedfish.cpp.KeyBytes;

/**
 * 
 * @author *-xguo0<@
 */
class Boundary {
    static String[] _symbols = new String[] {".", "", "'"};
    
    long pKey;
    int mark;

    public Boundary() {
    }
    
    public Boundary(long pKey, int mark) {
        this.pKey = pKey;
        this.mark = mark;
    }

    public boolean in(Range range) {
        return range.contains(this);
    }
    
    static String toString(long pKey, int mark) {
        String key = (pKey != 0) ? KeyBytes.toString(pKey) : "";
        String result = String.format("%s%s", key, _symbols[mark+1]);
        return result;
    }
    
    @Override
    public String toString() {
        String result = toString(this.pKey, this.mark);
        return result;
    }
}
