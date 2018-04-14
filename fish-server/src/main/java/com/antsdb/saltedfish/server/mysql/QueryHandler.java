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

import java.nio.CharBuffer;

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
        CharBuffer sql= packet.getSql();
        if (sql == null) {
            serverHandler.writeErrMessage(ctx, MysqlErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND, "Empty query.");
        } 
        else {
            serverHandler.session.run(sql, null, (result)-> {
                Helper.writeResonpse(ctx, serverHandler, result, true);
            });
        }
    }
}