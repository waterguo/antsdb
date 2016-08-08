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

import com.antsdb.saltedfish.lexer.MysqlParser.Show_columns_stmtContext;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.ObjectName;

public class Show_columns_stmtGenerator extends Generator<Show_columns_stmtContext>{

    // SHOW COLUMNS FROM table_name_ FROM namespace    
    @Override
    public Instruction gen(GeneratorContext ctx, Show_columns_stmtContext rule) throws OrcaException {
        
        String ns = "";
        String table = rule.table_name_().getText();
        boolean full = (rule.K_FULL() != null);
        
        // Use namespace parsed from table name like "TEST.table"
        ObjectName tableName = TableName.parse(ctx, rule.table_name_());
        if (tableName != null) {
            ns = tableName.getNamespace();
            table = tableName.table;
        }

        // Use explicit namespace if specified 
        if (rule.identifier() != null){
            ns = Utils.getIdentifier(rule.identifier());
        }

        return new ShowColumns(ns, table, full, Utils.getLiteralValue(rule.string_value()));
    }   
}
