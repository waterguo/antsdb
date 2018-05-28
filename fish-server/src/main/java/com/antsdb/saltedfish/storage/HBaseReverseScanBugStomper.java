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
package com.antsdb.saltedfish.storage;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

/**
 * hbase has a stupid bug that it ignores the exclusiveness of the start row when the scan direction is reversed. this
 * class is to work around that bug.
 * 
 * @author *-xguo0<@
 */
class HBaseReverseScanBugStomper implements ResultScanner {

    private ResultScanner upstream;
    private boolean isFirstRow = true;
    private byte[] startKey;

    HBaseReverseScanBugStomper(ResultScanner upstream, byte[] start) {
        this.upstream = upstream;
        this.startKey = start;
    }

    @Override
    public Result next() throws IOException {
        Result result = this.upstream.next();
        if (!this.isFirstRow) {
            return result;
        }
        if (result == null) {
            return result;
        }
        if (result.isEmpty()) {
            return result;
        }
        this.isFirstRow = false;
        byte[] rowkey = result.getRow();
        if (this.startKey.length-1 != rowkey.length) {
            return result;
        }
        for (int i=0; i<this.startKey.length-1; i++) {
            if (this.startKey[i] != rowkey[i]) {
                return result;
            }
        }
        result = this.upstream.next();
        return result;
    }

    @Override
    public Result[] next(int nbRows) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public Iterator<Result> iterator() {
        throw new NotImplementedException();
    }

    @Override
    public void close() {
        this.upstream.close();
    }
}
