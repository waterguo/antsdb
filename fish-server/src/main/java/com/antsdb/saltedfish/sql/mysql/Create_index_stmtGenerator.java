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

import com.antsdb.saltedfish.lexer.MysqlParser.Create_index_stmtContext;
import com.antsdb.saltedfish.sql.DdlGenerator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.CreateIndex;
import com.antsdb.saltedfish.sql.vdm.Flow;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.MysqlUpgradeIndexToPrimaryKey;
import com.antsdb.saltedfish.sql.vdm.ObjectName;

public class Create_index_stmtGenerator extends DdlGenerator<Create_index_stmtContext>{

    @Override
    public Instruction gen(GeneratorContext ctx, Create_index_stmtContext rule)
    throws OrcaException {
        String indexName = Utils.getIdentifier(rule.index_name().identifier());
        ObjectName tableName = TableName.parse(ctx, rule.table_name_());
        List<String> columns = new ArrayList<String>();
        rule.indexed_column_def().forEach((it) -> columns.add(Utils.getIdentifier(it.indexed_column().identifier())));
        boolean isUnique = rule.K_UNIQUE() != null;
        boolean createIfNotExists = rule.K_IF() != null && rule.K_NOT() != null && rule.K_EXISTS() != null;
        boolean isFullText = rule.K_FULLTEXT() != null;
        Instruction result = new CreateIndex(indexName, isFullText, isUnique, createIfNotExists, tableName, columns);
        if (isUnique) {
        	Flow flow = new Flow();
        	flow.add(result);
        	flow.add(new MysqlUpgradeIndexToPrimaryKey(tableName, indexName));
        	result = flow;
        }
        return result;
    }

}
