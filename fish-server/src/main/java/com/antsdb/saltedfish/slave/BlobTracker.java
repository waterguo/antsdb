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
package com.antsdb.saltedfish.slave;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 
 * @author *-xguo0<@
 */
class BlobTracker {
    Map<Long, Long> rowByTrxid = new HashMap<>();
    
    void add(long trxid, long pRow) {
        this.rowByTrxid.put(trxid, pRow);
    }
    
    void end(long trxid) {
        this.rowByTrxid.remove(trxid);
    }
    
    void freeTo(long trxid) {
        for (Iterator<Map.Entry<Long, Long>> it = rowByTrxid.entrySet().iterator();it.hasNext();) {
            Map.Entry<Long, Long> entry = it.next();
            long key = entry.getKey();
            if ((key < 0) && (key > trxid)) {
                it.remove();
            }
        }
    }

    long get(long trxid) {
        Long result = this.rowByTrxid.get(trxid);
        return (result == null) ? 0 : result;
    }
}
