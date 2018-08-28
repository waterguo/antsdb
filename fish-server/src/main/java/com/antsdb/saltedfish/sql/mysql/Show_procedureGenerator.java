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

import com.antsdb.saltedfish.lexer.MysqlParser.Show_procedureContext;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.Instruction;

/**
 * 
 * @author *-xguo0<@
 */
public class Show_procedureGenerator extends Generator<Show_procedureContext> {

    @Override
    public Instruction gen(GeneratorContext ctx, Show_procedureContext rule) throws OrcaException {
        String like = null;
        if (rule.K_LIKE() != null) {
            like = Utils.getLiteralValue(rule.string_value());
        }
        CursorMaker result = new ShowProcedure(like);
        return result;
    }

}
