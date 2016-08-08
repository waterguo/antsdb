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
package com.antsdb.saltedfish.server.mysql;

import io.netty.channel.ChannelHandlerContext;

import org.antlr.v4.runtime.CharStream;
import org.slf4j.Logger;

import com.antsdb.saltedfish.server.mysql.packet.QueryPacket;
import com.antsdb.saltedfish.server.mysql.util.MysqlErrorCode;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * @author roger
 */
public class QueryHandler {

    static Logger _log = UberUtil.getThisLogger();
    private MysqlServerHandler serverHandler;
    
    public QueryHandler(MysqlServerHandler severHandler) {
        this.serverHandler = severHandler;
    }

    public void query(ChannelHandlerContext ctx, QueryPacket packet) throws Exception {
        CharStream sql= packet.getSql();
        Object result = null;
        if (sql == null) {
            serverHandler.writeErrMessage(ctx, MysqlErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND, "Empty query.");
        } 
        else {
            result = serverHandler.session.run(sql);
        	Helper.writeResonpse(ctx, serverHandler, result, true);
        }
    }
}