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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.nosql.LogDependency;
import com.antsdb.saltedfish.slave.DbUtils;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.util.MysqlJdbcUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class Member implements LogDependency {
    public String user;
    public String password;
    private String gossip;
    public BelugaThread thread;
    public boolean load = false;
    public boolean init = false;
    public int nThreads = 4;
    public boolean warmer = false;
    QuorumNode qnode;
    volatile long lp;
    private Pod pod;
    
    public Member(Pod pod) {
        this.pod = pod;
    }
    
    public void setState(BelugaState state, String gossip) throws Exception {
        this.pod.getQuorum().setState(qnode.serverId, state);
        this.qnode.state = state;
        this.gossip = gossip;
    }
    
    public BelugaState getState() {
        /*
        if (this.state != BelugaState.LIVE) {
            return this.state;
        }
        if (this.thread.replicator.getError() != null) {
            return BelugaState.ERROR;
        }
        else {
            return BelugaState.LIVE;
        }
        */
        return this.qnode.state;
    }

    public String getGossip() {
        if (getState() != BelugaState.ACTIVE) {
            return this.gossip;
        }
        if (this.thread.replicator.getError() != null) {
            return this.thread.replicator.getError().getMessage();
        }
        else {
            return null;
        }
    }
    
    public String getEndpoint() {
        return this.qnode.endpoint;
    }
    
    public String getHost() {
        String[] result = StringUtils.split(getEndpoint(), ':');
        return result[0];
    }
    
    public int getPort() {
        String[] result = StringUtils.split(getEndpoint(), ':');
        return result.length == 0 ? 3306 : Integer.parseInt(result[1]);
    }

    public void stop() {
        if (this.thread == null) {
            return;
        }
        if (this.thread.isAlive()) {
            this.thread.replicator.close();
            this.thread.interrupt();
            try {
                this.thread.join(5000);
            }
            catch (InterruptedException x) {
            }
        }
        if (this.thread.isAlive()) {
            throw new OrcaException("unable to stop thread {}", this.thread.getName());    
        }
        this.thread = null;
    }

    public Connection createConnection() throws SQLException {
        String url = MysqlJdbcUtil.getUrl(this.getHost(), this.getPort(), null);
        Connection result = DriverManager.getConnection(url, this.user, this.password);
        DbUtils.execute(result, "SET @@antsdb_slave_replication_session='true'");
        return result;
    }

    @Override
    public long getLogPointer() {
        if (this.thread != null) {
            if (this.thread.replicator != null) {
                return this.thread.replicator.getLogPointer();
            }
        }
        return 0;
    }

    @Override
    public String getName() {
        return this.getEndpoint();
    }

    @Override
    public List<LogDependency> getChildren() {
        return Collections.emptyList();
    }

    public String getOptions() {
        StringBuffer buf = new StringBuffer();
        if (this.warmer) {
            buf.append("warmer");
        }
        return buf.toString();
    }

    public long getServerId() {
        return this.qnode.serverId;
    }
}
