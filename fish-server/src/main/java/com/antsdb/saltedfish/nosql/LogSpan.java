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

import java.util.Collection;

import com.antsdb.saltedfish.util.LongLong;

/**
 * 
 * @author *-xguo0<@
 */
public interface LogSpan {
    /**
     * 
     * @return null if the object is empty
     */
    public LongLong getLogSpan();
    
    public static LongLong union(Collection<LogSpan> list) {
        LongLong result = null;
        for (LogSpan i: list) {
            LongLong span = i.getLogSpan();
            if (span == null) {
                continue;
            }
            if (result == null) {
                result = new LongLong(span.x, span.y);
                continue;
            }
            result.x = Math.min(result.x, span.x);
            result.y = Math.max(result.y, span.y);
        }
        return result;
    }

    public static LongLong union(LongLong xx, LongLong yy) {
        if (xx == null) {
            return yy;
        }
        if (yy == null) {
            return xx;
        }
        LongLong result = new LongLong(0, 0);
        result.x = Math.min(xx.x, yy.x);
        result.y = Math.max(xx.y, yy.y);
        return result;
    }
}
