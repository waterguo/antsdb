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
package com.antsdb.saltedfish.nosql;

import java.io.File;
import java.io.IOException;

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
    public void close() throws IOException;
    public boolean supportReplication();
}
