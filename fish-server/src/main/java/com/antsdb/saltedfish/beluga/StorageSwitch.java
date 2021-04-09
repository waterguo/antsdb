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

import java.io.File;
import java.util.List;

import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.ConfigService;
import com.antsdb.saltedfish.nosql.HColumnRow;
import com.antsdb.saltedfish.nosql.Replicable;
import com.antsdb.saltedfish.nosql.StorageEngine;
import com.antsdb.saltedfish.nosql.StorageTable;
import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.util.LongLong;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * this class is used to silence storage mutations. we don't want slave to send anything to the storage 
 * 
 * @author *-xguo0<@
 */
public class StorageSwitch implements StorageEngine {
    private static final Logger _log = UberUtil.getThisLogger();
    
    private StorageEngine upstream;
    volatile boolean isMuted;

    public StorageSwitch(StorageEngine upstream) {
        this.upstream = upstream;
    }

    public void muteChanges(boolean value) {
        if (this.isMuted != value) {
            this.isMuted = value;
            _log.debug("mute changes: {}", value);
        }
    }
    
    @Override
    public void setEndSpacePointer(long sp) {
        this.upstream.setEndSpacePointer(sp);
    }

    @Override
    public void checkpoint() throws Exception {
        this.upstream.checkpoint();
    }

    @Override
    public LongLong getLogSpan() {
        return this.upstream.getLogSpan();
    }

    @Override
    public void open(File home, ConfigService config, boolean isMutable) throws Exception {
        this.upstream.open(home, config, isMutable);
    }

    @Override
    public StorageTable getTable(int id) {
        return this.upstream.getTable(id);
    }

    @Override
    public StorageTable createTable(SysMetaRow meta) {
        if (!this.isMuted) {
            return this.upstream.createTable(meta);
        }
        else {
            syncTable(meta);
            return getTable(meta.getTableId());
        }
    }

    @Override
    public boolean deleteTable(int id) {
        if (!this.isMuted) {
            return this.upstream.deleteTable(id);
        }
        else {
            return true;
        }
    }

    @Override
    public void createNamespace(String name) {
        if (!this.isMuted) {
            this.upstream.createNamespace(name);
        }
    }

    @Override
    public void deleteNamespace(String name) {
        if (!this.isMuted) {
            this.upstream.deleteNamespace(name);
        }
    }

    @Override
    public void syncTable(SysMetaRow meta) {
        this.upstream.syncTable(meta);
    }

    @Override
    public Replicable getReplicable() {
        return new StorageSwitchReplicable(this, this.upstream.getReplicable());
    }

    @Override
    public boolean exist(int tableId) {
        return this.upstream.exist(tableId);
    }

    @Override
    public void gc(long timestamp) {
        this.upstream.gc(timestamp);
    }

    @Override
    public boolean isTransactionRecoveryRequired() {
        return this.upstream.isTransactionRecoveryRequired();
    }

    @Override
    public void close() throws Exception {
        this.upstream.close();
    }

    @Override
    public void createColumn(int tableId, int columnId, String name, int type) {
        this.upstream.createColumn(tableId, columnId, name, type);
    }

    @Override
    public void deleteColumn(int tableId, int columnId, String columnName) {
        this.upstream.deleteColumn(tableId, columnId, columnName);
    }

    @Override
    public void postSchemaChange(SysMetaRow table, List<HColumnRow> columns) {
        this.upstream.postSchemaChange(table, columns);
    }
}
