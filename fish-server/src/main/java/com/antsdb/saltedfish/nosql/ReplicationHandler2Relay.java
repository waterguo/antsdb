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

/**
 * 
 * @author *-xguo0<@
 */
public class ReplicationHandler2Relay implements ReplicationHandler2 {
    protected ReplicationHandler2 downstream;
    
    public ReplicationHandler2Relay(ReplicationHandler2 downstream) {
        this.downstream = downstream;
    }
    
    @Override
    public void putRow(int tableId, long pRow, long version, long pLogRow, long lpLogRow) throws Exception {
        this.downstream.putRow(tableId, pRow, version, pLogRow, lpLogRow);
    }

    @Override
    public void putIndex(int tableId, long pIndexKey, long pIndex, long version, long pLogIndex, long lpLogIndex) 
    throws Exception {
        this.downstream.putIndex(tableId, pIndexKey, pIndex, version, pLogIndex, lpLogIndex);
    }

    @Override
    public void deleteRow(int tableId, long pKey, long version, long pLogDeleteRow, long lpLogDeleteRow) 
    throws Exception {
        this.downstream.deleteRow(tableId, pKey, version, pLogDeleteRow, lpLogDeleteRow);
    }

    @Override
    public void deleteIndex(int tableId, long pKey, long version, long pLogDelete, long lpDelete) throws Exception {
        this.downstream.deleteIndex(tableId, pKey, version, pLogDelete, lpDelete);
    }

    @Override
    public void flush(long lpRows, long lpIndexes) throws Exception {
        this.downstream.flush(lpRows, lpIndexes);    
    }

    @Override
    public void all(long pEntry, long lpEntry) throws Exception {
        this.downstream.all(pEntry, lpEntry);
    }

    @Override
    public void commit(long pEntry, long lpEntry) throws Exception {
        this.downstream.commit(pEntry, lpEntry);
    }

    @Override
    public void rollback(long pEntry, long lpEntry) throws Exception {
        this.downstream.rollback(pEntry, lpEntry);
    }

    @Override
    public void message(long pEntry, long lpEntry) throws Exception {
        this.downstream.message(pEntry, lpEntry);
    }

    @Override
    public void transactionWindow(long pEntry, long lpEntry) throws Exception {
        this.downstream.transactionWindow(pEntry, lpEntry);
    }

    @Override
    public void timestamp(long pEntry, long lpEntry) {
        this.downstream.timestamp(pEntry, lpEntry);
    }

    @Override
    public void ddl(long pEntry, long lpEntry) throws Exception {
        this.downstream.ddl(pEntry, lpEntry);
    }
}
