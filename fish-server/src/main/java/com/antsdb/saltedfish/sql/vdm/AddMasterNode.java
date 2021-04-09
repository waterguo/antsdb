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
package com.antsdb.saltedfish.sql.vdm;

import java.net.InetAddress;
import java.sql.Connection;
import java.util.Map;
import java.util.Properties;

import com.antsdb.saltedfish.beluga.Member;
import com.antsdb.saltedfish.beluga.Pod;
import com.antsdb.saltedfish.slave.DbUtils;
import com.antsdb.saltedfish.slave.JdbcReplicator;
import com.antsdb.saltedfish.sql.OrcaException;

/**
 * 
 * @author *-xguo0<@
 */
public class AddMasterNode extends Statement {

    private boolean configTarget = true;
    private Properties props;

    public AddMasterNode(Properties props) {
        this.props = props;
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters params) {
        Member member = parse();
        if (member.getEndpoint().indexOf(':') < 0) {
            throw new OrcaException("port is not specified");
        }
        if (ctx.getHumpback().getServerId() >= 2) {
            throw new OrcaException("this server id must be less than 2 to add another master");
        }
        Pod pod = ctx.getOrca().getBelugaPod();
        //member.serverId = serverId;
        pod.add(member);
        
        return null;
    }

    private Member parse() {
        Member result = new Member(null);
        result.init = true;
        //result.endpoint = this.props.getProperty("endpoint");
        result.user = this.props.getProperty("user");
        result.password = this.props.getProperty("password");
        result.load = Boolean.parseBoolean(this.props.getProperty("load", "false"));
        result.nThreads = Integer.parseInt(this.props.getProperty("threads", "4"));
        result.warmer = Boolean.parseBoolean(this.props.getProperty("warmer", "false"));
        this.configTarget = Boolean.parseBoolean(this.props.getProperty("config", "true"));
        return result;
    }

    private long initTarget(VdmContext ctx, Member member) {
        Connection conn = null;
        try {
            conn = member.createConnection();
            Map<String, Object> row;
            /*
            row = DbUtils.firstRow(conn, "SELECT * FROM antsdb.cluster_status WHERE SERVER_ID=?", myServerId);
            if (row != null) {
                throw new OrcaException("this server is already in target's cluster");
            }
            */
            
            // get the server id of the target
            
            row = DbUtils.firstRow(conn, "SELECT @@server_id as value");
            if (row == null) {
                throw new OrcaException("server id is not found");
            }
            if (row.get("value") == null) {
                throw new OrcaException("server id is not found");
            }
            long result = Long.parseLong(row.get("value").toString());
            if (result == ctx.getHumpback().getServerId()) {
                throw new OrcaException("target server id and this server id must be different");
            }
            
            // setup book mark tables
            
            JdbcReplicator.setup(conn, ctx.getHumpback().getServerId(), ctx.getHumpback().getGobbler().getLatestSp());
            
            // add this node to the target
            
            if (configTarget) {
                String port = ctx.getHumpback().getConfig().getProperty("fish.port", "3306");
                String host = InetAddress.getLocalHost().getHostName();
                String sql = String.format(".add external master node endpoint='%s:%s' config='false' warmer='%s'", 
                                           host, 
                                           port,
                                           member.warmer);
                DbUtils.execute(conn, sql);
            }
            
            // done 
            
            return result;
        }
        catch (Exception x) {
            throw new OrcaException(x);
        }
        finally {
            DbUtils.closeQuietly(conn);
        }
    }
}
