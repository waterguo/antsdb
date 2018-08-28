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
package com.antsdb.saltedfish.sql.mysql;

import org.apache.directory.api.util.exception.NotImplementedException;

import com.antsdb.saltedfish.lexer.MysqlParser.Kill_stmtContext;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.KillSession;

/**
 * 
 * @author *-xguo0<@
 */
public class Kill_stmtGenerator extends Generator<Kill_stmtContext> {

    @Override
    public Instruction gen(GeneratorContext ctx, Kill_stmtContext rule) throws OrcaException {
        boolean isQuery = rule.K_QUERY() != null;
        if (isQuery) {
            throw new NotImplementedException();
        }
        int sessionId = Integer.parseInt(rule.number_value().getText());
        return new KillSession(sessionId);
    }

}
