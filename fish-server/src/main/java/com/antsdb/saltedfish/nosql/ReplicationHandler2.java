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
public interface ReplicationHandler2 {
    
    /**
     * 
     * @param pRow: pointing to a Row object
     * @param version
     * @param lpLogRow: pointing to a RowUpdateEntry2 object
     * @throws Exception
     */
    default void putRow(int tableId, long pRow, long version, long pEntry, long lpEntry) throws Exception {
    }
    
    /**
     * 
     * @param pIndexKey: pointing to a KeyBytes object
     * @param pIndex: pointing to a KeyBytes object
     * @param version
     * @param lpLogIndex: pointing to a IndexEntry2 object
     * @throws Exception
     */
    default void putIndex(int tableId, long pIndexKey, long pIndex, long version, long pEntry, long lpEntry) 
    throws Exception {
    }
    
    default void deleteRow(int tableId, long pKey, long version, long pEntry, long lpEntry) 
    throws Exception {
    }
    
    default void deleteIndex(int tableId, long pKey, long version, long pEntry, long lpEntry) throws Exception {
    }
    
    /**
     * inform the handler to flush buffered data
     * 
     * @param lpRows  log pointer to the end of rows
     * @param lpIndexes log pointer to the end of index data
     * @throws Exception
     */
    default void flush(long lpRows, long lpIndexes) throws Exception{
    }
    
    default public void all(long pEntry, long lpEntry) throws Exception {
    }
    
    default public void commit(long pEntry, long lpEntry) throws Exception {
    }
    
    default public void rollback(long pEntry, long lpEntry) throws Exception {
    }
    
    default public void message(long pEntry, long lpEntry) throws Exception {
    }
    
    default public void transactionWindow(long pEntry, long lpEntry) throws Exception {
    }

    default public void timestamp(long pEntry, long lpEntry) {
    }

    default public void ddl(long pEntry, long lpEntry) throws Exception {
    }
}
