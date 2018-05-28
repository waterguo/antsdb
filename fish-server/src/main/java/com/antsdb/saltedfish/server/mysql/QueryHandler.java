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
package com.antsdb.saltedfish.server.mysql;

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
    private MysqlSession mysession;
    
    public QueryHandler(MysqlSession mysession) {
        this.mysession = mysession;
    }

    public void query(QueryPacket packet) throws Exception {
        CharBuffer sql= packet.getSql();
        query(sql);
    }
    
    public void query(CharBuffer sql) throws Exception {
        if (sql == null) {
            throw new ErrorMessage(MysqlErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND, "Empty query.");
        } 
        else {
            mysession.session.run(sql, null, (result)-> {
                Helper.writeResonpse(this.mysession.out, this.mysession, result, true);
            });
        }
    }
}