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
package com.antsdb.saltedfish.beluga;

import com.antsdb.saltedfish.nosql.ReplicationHandler2;

/**
 * 
 * @author *-xguo0<@
 */
public class StorageSwitchReplayHandler implements ReplicationHandler2 {

    private StorageSwitch owner;
    private ReplicationHandler2 upstream;

    public StorageSwitchReplayHandler(StorageSwitch owner, ReplicationHandler2 upstream) {
        this.owner = owner;
        this.upstream = upstream;
    }

    
    @Override
    public void putRow(int tableId, long pRow, long version, long pEntry, long lpEntry) throws Exception {
        if (!this.owner.isMuted) {
            this.upstream.putRow(tableId, pRow, version, pEntry, lpEntry);
        }
    }


    @Override
    public void putIndex(int tableId, long pIndexKey, long pIndex, long version, long pEntry, long lpEntry)
    throws Exception {
        if (!this.owner.isMuted) {
            this.upstream.putIndex(tableId, pIndexKey, pIndex, version, pEntry, lpEntry);
        }
    }


    @Override
    public void deleteRow(int tableId, long pKey, long version, long pEntry, long lpEntry) throws Exception {
        if (!this.owner.isMuted) {
            this.upstream.deleteRow(tableId, pKey, version, pEntry, lpEntry);
        }
    }


    @Override
    public void deleteIndex(int tableId, long pKey, long version, long pEntry, long lpEntry) throws Exception {
        if (!this.owner.isMuted) {
            this.upstream.deleteIndex(tableId, pKey, version, pEntry, lpEntry);
        }
    }


    @Override
    public void flush(long lpRows, long lpIndexes) throws Exception {
        if (!this.owner.isMuted) {
            this.upstream.flush(lpRows, lpIndexes);
        }
    }

    @Override
    public void all(long pEntry, long lpEntry) throws Exception {
        if (!this.owner.isMuted) {
            this.upstream.all(pEntry, lpEntry);
        }
    }


    @Override
    public void commit(long pEntry, long lpEntry) throws Exception {
        if (!this.owner.isMuted) {
            this.upstream.commit(pEntry, lpEntry);
        }
    }


    @Override
    public void rollback(long pEntry, long lpEntry) throws Exception {
        if (!this.owner.isMuted) {
            this.upstream.rollback(pEntry, lpEntry);
        }
    }


    @Override
    public void message(long pEntry, long lpEntry) throws Exception {
        if (!this.owner.isMuted) {
            this.upstream.message(pEntry, lpEntry);
        }
    }

    @Override
    public void transactionWindow(long pEntry, long lpEntry) throws Exception {
        if (!this.owner.isMuted) {
            this.upstream.transactionWindow(pEntry, lpEntry);
        }
    }


    @Override
    public void timestamp(long pEntry, long lpEntry) {
        if (!this.owner.isMuted) {
            this.upstream.timestamp(pEntry, lpEntry);
        }
    }


    @Override
    public void ddl(long pEntry, long lpEntry) throws Exception {
        if (!this.owner.isMuted) {
            this.upstream.ddl(pEntry, lpEntry);
        }
    }
}
