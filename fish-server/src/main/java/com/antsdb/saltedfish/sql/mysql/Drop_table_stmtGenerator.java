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

import java.util.ArrayList;
import java.util.List;

import com.antsdb.saltedfish.lexer.MysqlParser.Drop_table_stmtContext;
import com.antsdb.saltedfish.sql.DdlGenerator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.DropTable;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.ObjectName;

public class Drop_table_stmtGenerator extends DdlGenerator<Drop_table_stmtContext>{

    @Override
    public Instruction gen(GeneratorContext ctx, Drop_table_stmtContext rule) throws OrcaException {
    	List<ObjectName> list = new ArrayList<>();
    	rule.table_names_().table_name_().forEach(it -> list.add(TableName.parse(ctx, it)));;
        
        boolean ifExist = rule.K_EXISTS() != null;
        DropTable drop = new DropTable(list, ifExist);
        return drop;
    }

}
