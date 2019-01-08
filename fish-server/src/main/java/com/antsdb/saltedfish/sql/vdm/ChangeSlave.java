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

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.slave.JdbcReplicator;
import com.antsdb.saltedfish.slave.SlaveReplicator;
import com.antsdb.saltedfish.sql.OrcaException;

/**
 * 
 * @author *-xguo0<@
 */
public class ChangeSlave extends Statement {

    private Properties props;

    public ChangeSlave(Properties props) {
        this.props = props;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        String host = props.getProperty("host");
        String port = props.getProperty("port", "3306");
        String user = props.getProperty("user");
        String password = props.getProperty("password");
        String pos = props.getProperty("pos");
        
        // check slave replicator
        
        if (ctx.getOrca().getHumpback().getSlaveReplicator() != null) {
            throw new OrcaException("slave replicator needs to be stopped");
        }
        
        // check connection parameters
        
        if (StringUtils.isEmpty(host)) {
            throw new OrcaException("HOST is not specified");
        }
        if (StringUtils.isEmpty(port)) {
            port = "3306";
        }
        if (StringUtils.isEmpty(pos)) {
            throw new OrcaException("POS is not specified");
        }
        
        // setup bookmark 

        String url = String.format("jdbc:mysql://%s:%s", host, port);
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(url, user, password);
            JdbcReplicator.setup(conn, ctx.getHumpback().getServerId(), Long.parseLong(pos));
            conn.close();
        }
        catch (Exception x) {
            throw new OrcaException("unable to connect to the slave -{}", x.toString()); 
        }
        
        // done
        
        ctx.getHumpback().setConfig(ctx.getHSession(), SlaveReplicator.KEY_HOST, host);
        ctx.getHumpback().setConfig(ctx.getHSession(), SlaveReplicator.KEY_PORT, port);
        ctx.getHumpback().setConfig(ctx.getHSession(), SlaveReplicator.KEY_USER, user);
        ctx.getHumpback().setConfig(ctx.getHSession(), SlaveReplicator.KEY_PASSWORD, password);
        
        return null;
    }
}
