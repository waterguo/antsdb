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
package com.antsdb.saltedfish.sql.mysql;

import com.antsdb.saltedfish.lexer.MysqlParser.Drop_user_stmtContext;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.DropUser;
import com.antsdb.saltedfish.sql.vdm.Instruction;

/**
 * 
 * @author *-xguo0<@
 */
public class Drop_user_stmtGenerator extends Generator<Drop_user_stmtContext> {

    @Override
    public Instruction gen(GeneratorContext ctx, Drop_user_stmtContext rule) throws OrcaException {
        String user = Utils.getLiteralValue(rule.string_value());
        return new DropUser(user, rule.K_EXISTS() != null);
    }

}
