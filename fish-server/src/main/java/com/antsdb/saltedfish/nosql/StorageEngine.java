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

import java.io.File;
import java.util.List;

/**
 * 
 * @author *-xguo0<@
 */
public interface StorageEngine extends Synchronizable  {
    public void open(File home, ConfigService config, boolean isMutable) throws Exception;
    public StorageTable getTable(int id);
    public StorageTable createTable(SysMetaRow meta);
    public boolean deleteTable(int id);
    public void createNamespace(String name);
    public void deleteNamespace(String name);
    public void syncTable(SysMetaRow meta);
    public Replicable getReplicable();
    
    /**
     * check if the table physically exist in the storage
     * 
     * @return true if it exists, false if it doesnt. throw exceptions if anything else
     */
    public boolean exist(int tableId);
    /**
     * gc garbages that is older than the specified time stamp
     * @param timestamp
     */
    
    public void gc(long timestamp);
    /**
     * determines if we need to recover transaction at boot time. required by real-time replication
     * 
     * @return
     */
    public boolean isTransactionRecoveryRequired();
    
    public void close() throws Exception;

    /**
     * called when antsdb trying to create a new column on a table
     * 
     * @param tableId
     * @param columnId
     * @param name not null
     * @param type see class Value
     */
    default public void createColumn(int tableId, int columnId, String name, int type) {
    }
    
    /**
     * called when antsdb trying to delete a column on a table
     * @param tableId
     * @param columnId
     * @param columnName not null
     */
    default public void deleteColumn(int tableId, int columnId, String columnName) {
    }
    
    /**
     * called after CREATE TABLE or ALTER TABLE statements
     * @param table not null
     * @param columns not null
     */
    default public void postSchemaChange(SysMetaRow table, List<HColumnRow> columns) {};
}
