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

import com.antsdb.saltedfish.lexer.MysqlParser.Create_database_stmtContext;
import com.antsdb.saltedfish.sql.DdlGenerator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.CreateNamespace;
import com.antsdb.saltedfish.sql.vdm.Instruction;

public class Create_database_stmtGenerator extends DdlGenerator<Create_database_stmtContext>{

    @Override
    public Instruction gen(GeneratorContext ctx, Create_database_stmtContext rule) throws OrcaException {
        String name = Utils.getIdentifier(rule.identifier());
        boolean ifNotExists = rule.K_NOT() != null;
        CreateNamespace step = new CreateNamespace(name, ifNotExists);
        return step;
    }

}
