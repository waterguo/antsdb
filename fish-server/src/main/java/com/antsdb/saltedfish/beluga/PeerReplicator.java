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

import java.sql.Connection;

import com.antsdb.saltedfish.nosql.Gobbler.CommitEntry;
import com.antsdb.saltedfish.nosql.Gobbler.DdlEntry;
import com.antsdb.saltedfish.nosql.Gobbler.DeleteEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.DeleteRowEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.IndexEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.InsertEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.MessageEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.PutEntry2;
import com.antsdb.saltedfish.nosql.Gobbler.UpdateEntry2;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.slave.DbUtils;
import com.antsdb.saltedfish.slave.JdbcReplicator;

/**
 * 
 * @author *-xguo0<@
 */
public class PeerReplicator extends JdbcReplicator {
    Member member;
    
    public PeerReplicator(Humpback humpback, Member member, String prefix) 
    throws Exception {
        super(humpback, member.getHost(), String.valueOf(member.getPort()), member.user, member.password);
        this.member = member;
    }

    @Override
    public Connection createConnection() throws Exception {
        Connection conn = super.createConnection();
        DbUtils.execute(conn, "SET @@antsdb_slave_replication_session='true'");
        return conn;
    }

    @Override
    public void insert(InsertEntry2 entry) throws Exception {
        if (entry.getSessionId() < 0) {
            return;
        }
        super.insert(entry);
    }

    @Override
    public void update(UpdateEntry2 entry) throws Exception {
        if (entry.getSessionId() < 0) {
            return;
        }
        super.update(entry);
    }

    @Override
    public void put(PutEntry2 entry) throws Exception {
        if (entry.getSessionId() < 0) {
            return;
        }
        super.put(entry);
    }

    @Override
    public void delete(DeleteEntry2 entry) throws Exception {
        if (entry.getSessionId() < 0) {
            return;
        }
        super.delete(entry);
    }

    @Override
    public void deleteRow(DeleteRowEntry2 entry) throws Exception {
        if (entry.getSessionId() < 0) {
            return;
        }
        super.deleteRow(entry);
    }

    @Override
    public void index(IndexEntry2 entry) throws Exception {
        if (entry.getSessionId() < 0) {
            return;
        }
        super.index(entry);
    }

    @Override
    public void commit(CommitEntry entry) throws Exception {
        if (entry.getSessionId() < 0) {
            return;
        }
        super.commit(entry);
    }

    @Override
    public void ddl(DdlEntry entry) throws Exception {
        if (entry.getSessionId() < 0) {
            return;
        }
        super.ddl(entry);
    }

    @Override
    public void message(MessageEntry2 entry) throws Exception {
        if (entry.getSessionId() < 0) {
            return;
        }
        super.message(entry);
    }
}
