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

import com.antsdb.saltedfish.lexer.MysqlParser.*;
import com.antsdb.saltedfish.sql.DdlGenerator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.DropNamespace;
import com.antsdb.saltedfish.sql.vdm.Instruction;

public class Drop_database_stmtGenerator extends DdlGenerator<Drop_database_stmtContext>{

    @Override
    public Instruction gen(GeneratorContext ctx, Drop_database_stmtContext rule) throws OrcaException {
        String name = rule.any_name().getText();
        boolean ifExists = (rule.K_EXISTS() != null);
        return new DropNamespace(name, ifExists);
    }

}
