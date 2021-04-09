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
package com.antsdb.saltedfish.parquet;

import java.util.ArrayList;
import java.util.List;

import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.nosql.HColumnRow;
import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.sql.Orca;

/**
 * 
 * @author Frank Li<lizc@tg-hd.com>
 */
public class Mapping {
    final static Column[] SYSTEM_QUALIFIERS = new Column[100];

    public int tableId;
    public String ns;
    public String tableName;
    Column[] qualifiers;

    static {
        for (short i = 0; i < SYSTEM_QUALIFIERS.length; i++) {
            SYSTEM_QUALIFIERS[i] = Column.valueOf("x" + String.format("%02d", i), (int) Value.TYPE_BYTES);
        }
    }

    public Mapping(String sysns, SysMetaRow tableInfo, List<HColumnRow> columns) {
        this.tableId = tableInfo.getTableId();
        this.ns = tableInfo.getNamespace();
        this.ns = (this.ns.equals(Orca.SYSNS)) ? sysns : this.ns;
        this.tableName = tableInfo.getTableName();
        if ((columns != null) && (columns.size() > 0)) {
            int max = 0;
            for (HColumnRow i : columns) {
                if (i.isDeleted()) {
                    continue;
                }
                max = Math.max(max, i.getColumnPos());
            }
            this.qualifiers = new Column[max + 1];
            this.qualifiers[0] = Column.valueOf(Helper.SYS_COLUMN_ROWID_BYTES, (int) Value.TYPE_BYTES);
            for (HColumnRow i : columns) {
                if (i.isDeleted()) {
                    continue;
                }
                this.qualifiers[i.getColumnPos()] = Column.valueOf(i.getColumnName(), i.getType());
            }
        }
    }

    public Column[] getQualifiers() {
        return qualifiers;
    }

    public Column getColumn(int i) {
        if (this.qualifiers != null) {
            return (i < this.qualifiers.length) ? this.qualifiers[i] : null;
        }
        else {
            return SYSTEM_QUALIFIERS[i];
        }

    }

    public List<Column> getQualifiersByLists() {
        List<Column> columns = new ArrayList<>();
        if (qualifiers != null) {
            for (Column c : qualifiers) {
                columns.add(c);
            }
        }
        else {
            for (Column c : SYSTEM_QUALIFIERS) {
                columns.add(c);
            }
        }
        return columns;
    }

    @Override
    public String toString() {
        StringBuffer sqlBuffer = new StringBuffer();
        sqlBuffer.append("dbnameL" + ns).append("tbname:" + this.tableName + "-" + tableId);
        if (qualifiers != null) {
            for (Column c : qualifiers) {
                sqlBuffer.append(c.getName());
            }
        }
        else {
            sqlBuffer.append(" column is empty!");
        }
        return sqlBuffer.toString();
    }
}
