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

import java.nio.ByteBuffer;

import org.slf4j.Logger;

import com.antsdb.saltedfish.charset.Decoder;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.obs.ExternalQuerySession;
import com.antsdb.saltedfish.nosql.HumpbackSession;
import com.antsdb.saltedfish.server.mysql.util.MysqlErrorCode;
import com.antsdb.saltedfish.sql.SqlLogger;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * @author xgu0
 */
public class QueryHandler {

    static Logger _log = UberUtil.getThisLogger();
    private MysqlSession mysession;

    public QueryHandler(MysqlSession mysession) {
        this.mysession = mysession;
    }

    public void query(String sql) throws Exception {
        byte[] bytes = sql.getBytes();
        ByteBuffer buf = ByteBuffer.allocateDirect(bytes.length);
        buf.put(bytes);
        buf.flip();
        query(buf, Decoder.UTF8);
    }

    public void query(ByteBuffer sql, Decoder decoder) throws Exception {
        if (sql == null) {
            throw new ErrorMessage(MysqlErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND, "Empty query.");
        }
        else {
            if (sql.limit() > 0 && '!' == sql.get(0)) {
                Humpback humpback = this.mysession.session.getOrca().getHumpback();
                ExternalQuerySession.getInstance().run(
                        String.valueOf(this.mysession.session.getId()), 
                        humpback, 
                        sql, 
                        null, 
                        (result) -> {
                            HelperExternal.writeExternalResonpse(humpback,this.mysession.out, this.mysession, result);
                });
                return;
            }
            HumpbackSession hsession = this.mysession.session.getHSession();
            long startLp = hsession.getLastLp();
            mysession.session.run(sql, null, (result)-> {
                if (hsession.getLastLp() != startLp) {
                    SqlLogger logger = this.mysession.session.getOrca().getSqlLogger();
                    logger.logWrite(mysession.session, "query", result, sql);
                }
                Helper.writeResonpse(this.mysession.out, this.mysession, result, true);
            });
        }
    }
}