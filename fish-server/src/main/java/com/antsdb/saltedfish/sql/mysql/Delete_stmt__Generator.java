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

import com.antsdb.saltedfish.lexer.MysqlParser.Delete_stmt__Context;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.HumpbackDelete;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.util.BytesUtil;

/**
 * 
 * @author wgu0
 */
public class Delete_stmt__Generator extends Generator<Delete_stmt__Context> {

    @Override
    public Instruction gen(GeneratorContext ctx, Delete_stmt__Context rule) throws OrcaException {
        int tableId = Integer.parseInt(rule.number_value().getText());
        String hex = rule.STRING_LITERAL().getText();
        hex = hex.substring(1, hex.length()-1);
        byte[] key = BytesUtil.hexToBytes(hex);
        return new HumpbackDelete(tableId, key);
    }

}
