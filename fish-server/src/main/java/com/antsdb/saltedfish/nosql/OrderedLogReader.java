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

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.util.SizeConstants;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * read the log entries ordered by the row key instead of sequentially
 * 
 * @author *-xguo0<@
 */
public class OrderedLogReader {
    static Logger _log = UberUtil.getThisLogger();

    private Humpback humpback;
    private long lp;
    private HumpbackSession hsession;
    private long batchSize = SizeConstants.mb(256);

    public OrderedLogReader(Humpback humpback) {
        this.humpback = humpback;
        hsession = humpback.createSession(":log_reader");
    }
    public OrderedLogReader(Humpback humpback,long batchSize) {
        this.humpback = humpback;
        hsession = humpback.createSession(":log_reader");
        this.batchSize = batchSize;
    }
    
    public void setPosition(long value) {
        this.lp = value;
    }
    
    public long getPosition() {
        return this.lp;
    }
    
    public void setBatchSize(long value) {
        this.batchSize = value;
    }
    
    
    public void replay(ReplicationHandler2 callback) throws Exception {
        try {
            this.hsession.open();
            long end = this.humpback.getStartLogPointer() - 1;
            if (this.batchSize == 0) {
                replay(end, callback);
            }
            else {
                while (end > this.lp) {
                    long batchEnd = this.humpback.spaceman.plus(this.lp, this.batchSize, 0);
                    batchEnd = Math.min(end, batchEnd);
                    replay(batchEnd, callback);
                    this.lp = batchEnd + 1;
                }
            }
            this.lp = end + 1;
        }
        finally {
            this.hsession.close();
        }
    }
    
    private void replay(long end, ReplicationHandler2 callback) throws Exception {
        Collection<GTable> tables = this.humpback.getTables();
        for (GTable i : tables) {
            int tableId = i.getId();
            if (tableId < 0x100) {
                replay(end, i, callback);
            }
        }
        for (GTable i : tables) {
            if (i.getId() >= 0x100) {
                replay(end, i, callback);
            }
        }
    }
    
    private long replay(long end, GTable table, ReplicationHandler2 callback) throws Exception {
        long count = 0;
        long pLastKey = 0;
        try (RowIterator ii = table.scanDelta(this.lp, end)) {
            while (ii.next()) {
                if (pLastKey != 0) {
                    long pKey = ii.getKeyPointer(); 
                    if (KeyBytes.compare(pKey, pLastKey) <= 0) {
                        throw new IllegalArgumentException();
                    }
                }
                replay(table.getId(), table.getTableType(), ii, callback);
                count++;
                pLastKey = ii.getKeyPointer();
            }
        }
        return count;
    }
    
    public void replay_bak(ReplicationHandler2 callback) throws Exception {
        try {
            this.hsession.open();
            long end = this.humpback.getStartLogPointer() - 1;
            if (end > this.lp) {  
                for (GTable i : this.humpback.getTables()) {
                    RowIterator ii = i.scanDelta(this.lp, end);
                    while (ii.next()) {
                        replay(i.getId(), i.getTableType(), ii, callback);
                    }
                    ii.close();
                }
            }
            this.lp = end + 1;
        }
        finally {
            this.hsession.close();
        }
    }

    private void replay(int tableId,TableType tableType, RowIterator ii, ReplicationHandler2 callback) throws Exception {
        long version = ii.getVersion();
        long pKey = ii.getKeyPointer();
        long pRow = ii.getRowPointer();
        long lp = ii.getLogPointer();
        long p = this.humpback.getSpaceManager().toMemory(lp);
        if (pRow == 1) {
            if (tableType == TableType.DATA) {
                callback.deleteRow(tableId, pKey, version, p, lp);
            }
            else {
                callback.deleteIndex(tableId, pKey, version, p, lp);
            }
        }
        else {
            if (tableType == TableType.DATA) {
                //callback.putRow(tableId,pRow, version, lp - RowUpdateEntry2.getHeaderSize());
                callback.putRow(tableId, pRow, version, p, lp);
            }
            else {
                long pRowKey = ii.getRowKeyPointer();
                callback.putIndex(tableId, pKey, pRowKey, version, p, lp);
            }
        }
    }

}
