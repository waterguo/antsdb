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
package com.antsdb.saltedfish.sql.command;

import java.util.HashMap;
import java.util.Map;

import com.antsdb.saltedfish.lexer.FishParser.AssignmentContext;
import com.antsdb.saltedfish.lexer.FishParser.Change_slave_stmtContext;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.ChangeSlave;
import com.antsdb.saltedfish.sql.vdm.Instruction;

/**
 * 
 * @author *-xguo0<@
 */
public class Change_slave_stmtGenerator extends Generator<Change_slave_stmtContext> {

    @Override
    public Instruction gen(GeneratorContext ctx, Change_slave_stmtContext rule) throws OrcaException {
        Map<String, String> props = new HashMap<>();
        for (AssignmentContext i:rule.assignment()) {
            gen(props, i);
        }
        return new ChangeSlave(props);
    }

    private void gen(Map<String, String> props, AssignmentContext rule) {
        String key = rule.IDENTIFIER().getText();
        String value = rule.STRING_LITERAL().getText();
        value = value.substring(1, value.length()-1);
        props.put(key.toLowerCase(), value);
    }

}
