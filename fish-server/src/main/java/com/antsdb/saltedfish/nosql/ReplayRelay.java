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
public class ReplayRelay implements ReplayHandler {
    protected ReplayHandler downstream;
    
    @Override
    public void all(LogEntry entry) throws Exception {
        this.downstream.all(entry);
    }

    @Override
    public void commit(CommitEntry entry) throws Exception {
        this.downstream.commit(entry);
    }

    @Override
    public void rollback(RollbackEntry entry) throws Exception {
        this.downstream.rollback(entry);
    }

    @Override
    public void message(MessageEntry entry) throws Exception {
        this.downstream.message(entry);
    }

    @Override
    public void transactionWindow(TransactionWindowEntry entry) throws Exception {
        this.downstream.transactionWindow(entry);
    }

    @Override
    public void timestamp(TimestampEntry entry) throws Exception {
        this.downstream.timestamp(entry);
    }

    public ReplayRelay(ReplayHandler downstream) {
        this.downstream = downstream;
    }

    @Override
    public void insert(InsertEntry2 entry) throws Exception {
        this.downstream.insert(entry);
    }

    @Override
    public void update(UpdateEntry2 entry) throws Exception {
        this.downstream.update(entry);
    }

    @Override
    public void put(PutEntry2 entry) throws Exception {
        this.downstream.put(entry);
    }

    @Override
    public void index(IndexEntry2 entry) throws Exception {
        this.downstream.index(entry);
    }

    @Override
    public void delete(DeleteEntry2 entry) throws Exception {
        this.downstream.delete(entry);
    }

    @Override
    public void message(MessageEntry2 entry) throws Exception {
        this.downstream.message(entry);
    }

    @Override
    public void deleteRow(DeleteRowEntry2 entry) throws Exception {
        this.downstream.deleteRow(entry);
    }

    @Override
    public void ddl(DdlEntry entry) throws Exception {
        this.downstream.ddl(entry);
    }
    
}
