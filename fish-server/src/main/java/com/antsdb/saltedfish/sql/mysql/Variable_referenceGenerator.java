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

import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.lexer.MysqlParser.Variable_referenceContext;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.GetSessionVarible;
import com.antsdb.saltedfish.sql.vdm.Instruction;

public class Variable_referenceGenerator extends Generator<Variable_referenceContext> {
    @Override
    public Instruction gen(GeneratorContext ctx, Variable_referenceContext rule)
    throws OrcaException {
        String name = rule.USER_VARIABLE().getText();
        name = StringUtils.removeStart(name, "@");
        return new GetSessionVarible(name);
    }

}
