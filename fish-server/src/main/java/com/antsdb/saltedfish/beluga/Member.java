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

import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.HumpbackSession;
import com.antsdb.saltedfish.nosql.LogDependency;
import com.antsdb.saltedfish.slave.DbUtils;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.util.MysqlJdbcUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class Member implements LogDependency {
    public String endpoint;
    public String user;
    public String password;
    private BelugaState state = BelugaState.DISCONNECTED;
    private String gossip;
    public BelugaThread thread;
    public long serverId;
    public boolean load = false;
    public boolean init = false;
    public int nThreads = 4;
    public boolean warmer = false;
    
    public void setState(BelugaState state, String gossip) {
        this.state = state;
        this.gossip = gossip;
    }
    
    public BelugaState getState() {
        if (this.state != BelugaState.LIVE) {
            return this.state;
        }
        if (this.thread.replicator.getError() != null) {
            return BelugaState.ERROR;
        }
        else {
            return BelugaState.LIVE;
        }
    }

    public String getGossip() {
        if (this.state != BelugaState.LIVE) {
            return this.gossip;
        }
        if (this.thread.replicator.getError() != null) {
            return this.thread.replicator.getError().getMessage();
        }
        else {
            return null;
        }
    }
    
    public String getHost() {
        String[] result = StringUtils.split(endpoint, ':');
        return result[0];
    }
    
    public int getPort() {
        String[] result = StringUtils.split(endpoint, ':');
        return result.length == 0 ? 3306 : Integer.parseInt(result[1]);
    }

    public void stop() {
        if (this.thread == null) {
            return;
        }
        if (this.thread.isAlive()) {
            this.thread.replicator.close();
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

    public void save(Humpback humpback, HumpbackSession hsession, String prefix) {
        humpback.setConfig(hsession, prefix + this.serverId + "/endpoint", this.endpoint);
        humpback.setConfig(hsession, prefix + this.serverId + "/user", this.user);
        humpback.setConfig(hsession, prefix + this.serverId + "/password", this.password);
        humpback.setConfig(hsession, prefix + this.serverId + "/warmer", this.warmer);
    }

    public void load(Humpback humpback, String prefix) {
        prefix = prefix + this.serverId;
        this.endpoint = humpback.getConfig(prefix + "/endpoint");
        this.user = humpback.getConfig(prefix + "/user");
        this.password = humpback.getConfig(prefix + "/password");
        this.warmer = humpback.getConfigAsBoolean(prefix + "/warmer", false);
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
        return this.endpoint;
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
}
