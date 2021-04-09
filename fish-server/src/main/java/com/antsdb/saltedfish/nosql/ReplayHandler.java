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
 * implemented by caller
 * 
 * @author wgu0
 */
public interface ReplayHandler {
    default public void all(LogEntry entry) throws Exception {
    }
    
    public void insert(InsertEntry2 entry) throws Exception;
    
    public void update(UpdateEntry2 entry) throws Exception;
    
    public void put(PutEntry2 entry) throws Exception;
    
    public void index(IndexEntry2 entry) throws Exception;
    
    public void delete(DeleteEntry2 entry) throws Exception;
    
    default public void commit(CommitEntry entry) throws Exception {
    }
    
    default public void rollback(RollbackEntry entry) throws Exception {
    }
    
    default public void message(MessageEntry entry) throws Exception {
    }

    public void message(MessageEntry2 entry) throws Exception;
    
    default public void transactionWindow(TransactionWindowEntry entry) throws Exception {
    }

    default public void timestamp(TimestampEntry entry) throws Exception {
    }

    public void deleteRow(DeleteRowEntry2 entry) throws Exception;

    public void ddl(DdlEntry entry) throws Exception;
}
