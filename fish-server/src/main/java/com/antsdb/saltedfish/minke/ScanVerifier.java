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
package com.antsdb.saltedfish.minke;

import static com.antsdb.saltedfish.minke.BoundaryMark.*;

import com.antsdb.saltedfish.nosql.ScanOptions;
import com.antsdb.saltedfish.nosql.ScanResult;
import com.antsdb.saltedfish.nosql.ScanResultComparator;
import com.antsdb.saltedfish.nosql.StorageTable;

/**
 * 
 * @author *-xguo0<@
 */
class ScanVerifier {
    static void check(MinkeCacheTable mctable, Range range, boolean asc) {
        long options = 0;
        options = range.startMark == NONE ? options : ScanOptions.excludeStart(options);
        options = range.endMark == NONE ? options : ScanOptions.excludeEnd(options);
        options = asc ? options : ScanOptions.descending(options);
        StorageTable stable = mctable.stable;
        MinkeTable mtable = mctable.mtable;
        ScanResult srHbase = stable.scan(range.pKeyStart, range.pKeyEnd, 0); 
        ScanResult srMinke = stable.scan(range.pKeyStart, range.pKeyEnd, 0);
        boolean success = ScanResultComparator.compare(srHbase, srMinke, mtable.getType());
        if (!success) {
            throw new MinkeException(
                    "cache malfunction {} {} {}",
                    mctable.mtable.tableId,
                    range.toString(),
                    asc ? "asc" : "desc");
        }
    }
}
