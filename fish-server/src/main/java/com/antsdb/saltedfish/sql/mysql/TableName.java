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

import java.util.List;

import com.antsdb.saltedfish.lexer.MysqlParser.IdentifierContext;
import com.antsdb.saltedfish.lexer.MysqlParser.Table_name_Context;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.util.CodingError;

class TableName {

    public static ObjectName parse(GeneratorContext ctx, List<? extends IdentifierContext> rules) {
        if ((rules == null) || (rules.size() == 0)) {
            return null;
        }
        ObjectName tableName = new ObjectName();
        List<? extends IdentifierContext> names = rules;
        if (rules.size() == 1) {
            tableName.catalog = ctx.getSession().getCurrentCatalog();
            tableName.schema = ctx.getSession().getCurrentSchema();
            tableName.table = Utils.getIdentifier(names.get(0));
        }
        else if (names.size() == 2) {
            tableName.catalog = ctx.getSession().getCurrentCatalog();
            tableName.schema = Utils.getIdentifier(names.get(0));
            tableName.table = Utils.getIdentifier(names.get(1));
        }
        else if (names.size() == 3) {
            tableName.catalog = Utils.getIdentifier(names.get(0));
            tableName.schema = Utils.getIdentifier(names.get(1));
            tableName.table = Utils.getIdentifier(names.get(2));
        }
        else {
            throw new CodingError();
        }
        return tableName;
    }
    
    public static ObjectName parse(GeneratorContext ctx, Table_name_Context rule) {
        if (rule == null) {
            return null;
        }
        ObjectName tableName = new ObjectName();
        List<? extends IdentifierContext> names = rule.identifier();
        if (names.size() == 1) {
            tableName.catalog = ctx.getSession().getCurrentCatalog();
            tableName.schema = ctx.getSession().getCurrentSchema();
            tableName.table = Utils.getIdentifier(names.get(0));
        }
        else if (names.size() == 2) {
            tableName.catalog = ctx.getSession().getCurrentCatalog();
            tableName.schema = Utils.getIdentifier(names.get(0));
            tableName.table = Utils.getIdentifier(names.get(1));
        }
        else if (names.size() == 3) {
            tableName.catalog = Utils.getIdentifier(names.get(0));
            tableName.schema = Utils.getIdentifier(names.get(1));
            tableName.table = Utils.getIdentifier(names.get(2));
        }
        else {
            throw new CodingError();
        }
        return tableName;
    }

    public static TableMeta resolve(GeneratorContext ctx, Table_name_Context rule) {
        ObjectName tableName = parse(ctx, rule);
        TableMeta table = ctx.getSession().getOrca().getMetaService().getTable(
                ctx.getSession().getTransaction(), 
                tableName);
        return table;
    }
    
}
