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
package com.antsdb.saltedfish.sql.vdm;

import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.server.mysql.ErrorMessage;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;

/**
 * common checks
 * 
 * @author *-xguo0<@
 */
public class Checks {
    
    public static Object tableOrViewExist(Session session, ObjectName name) {
        return tableOrViewExist(session, name.getNamespace(), name.getTableName());
    }
    
    public static Object tableOrViewExist(Session session, String ns, String table) {
        Object result = session.getOrca().getSystemView(ns, table);
        if (result != null) {
            return result;
        }
        return tableExist(session, ns, table);
    }
    
    public static TableMeta tableExist(Session session, String ns, String table) throws OrcaException {
        Orca orca = session.getOrca();
        namespaceExist(orca, ns);
        TableMeta tableMeta = session.getTable(ns, table);
        if (tableMeta == null) {
            throw new OrcaException("table doesn't exist: " + ns + "." + table);
        }
        return tableMeta;
    }

    public static TableMeta tableExist(Session session, ObjectName name) throws OrcaException {
        return tableExist(session, name.getNamespace(), name.getTableName());
    }

    public static void tableNotExist(Session session, ObjectName tableName) throws OrcaException {
        tableNotExist(session, tableName.getNamespace(), tableName.getTableName());
    }
    
    public static void tableNotExist(Session session, String ns, String tableName) throws OrcaException {
        if (session.getTable(ns, tableName) != null) {
            throw new OrcaException("table exist: {}.{}", ns, tableName);
        }
    }

    public static String namespaceExist(Orca orca, String ns) throws OrcaException {
        String result = orca.getExternalNamespace(ns);
        if (result != null) {
            return result;
        }
        Humpback humpback = orca.getHumpback();
        result = humpback.getNamespace(ns);
        if (result != null) {
            return result;
        }
        else {
            throw new ErrorMessage(1049, "42000 Unknown database `" + ns + "`");
        }
    }
    
    public static void namespaceNotExist(Orca orca, String ns) throws OrcaException {
        Humpback humpback = orca.getHumpback();
        if (humpback.getNamespace(ns) != null) {
            throw new OrcaException("namespace already exist: " + ns);
        }
    }
    
    public static ColumnMeta columnExist(TableMeta table, String columnName) throws OrcaException {
        ColumnMeta column = table.getColumn(columnName);
        if (column == null) {
            throw new OrcaException("column doesn't exist: " + columnName);
        }
        return column;
    }

    public static IndexMeta indexExist(TableMeta table, String indexName) throws OrcaException {
        IndexMeta index = table.findIndex(indexName);
        if (index == null) {
            throw new OrcaException("index doesn't exist: " + indexName);
        }
        return index;
    }
}
