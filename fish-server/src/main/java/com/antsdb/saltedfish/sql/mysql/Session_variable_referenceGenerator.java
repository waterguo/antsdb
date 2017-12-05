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

import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.lexer.MysqlParser.Session_variable_referenceContext;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.GetSystemVarible;
import com.antsdb.saltedfish.sql.vdm.Instruction;

public class Session_variable_referenceGenerator extends Generator<Session_variable_referenceContext> {

    @Override
    public Instruction gen(GeneratorContext ctx, Session_variable_referenceContext rule)
    throws OrcaException {
        String name = rule.SESSION_VARIABLE().getText();
        name = StringUtils.removeStart(name, "@@");
        return new GetSystemVarible(name);
    }

}
