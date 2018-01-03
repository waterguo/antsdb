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
package com.antsdb.saltedfish.storage;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;

/**
 * 
 * @author *-xguo0<@
 */
class Mapping {
    final static byte[] SYSTEM_FAMILY = Helper.SYS_COLUMN_DATATYPE_BYTES;
    final static byte[] USER_FAMILY = Helper.DATA_COLUMN_FAMILY_BYTES;
    final static byte[][] SYSTEM_QUALIFIERS = new byte[100][];
    
    int tableId;
    String ns;
    String tableName;
    byte[][] qualifiers;
    
    static {
        for (short i=0; i<SYSTEM_QUALIFIERS.length; i++) {
            SYSTEM_QUALIFIERS[i] = Bytes.toBytes(i);
        }
    }
    
    Mapping(String sysns, SysMetaRow tableInfo, TableMeta tableMeta) {
        this.tableId = tableInfo.getTableId();
        this.ns = tableInfo.getNamespace();
        this.ns = (this.ns.equals(Orca.SYSNS)) ? sysns : this.ns; 
        this.tableName = tableInfo.getTableName();
        if (tableMeta != null) {
            this.qualifiers = new byte[tableMeta.getMaxColumnId()+1][];
            for (int i=0; i<=tableMeta.getMaxColumnId(); i++) {
                ColumnMeta column = tableMeta.getColumnByColumnId(i);
                if (column == null) {
                    if (i == 0) {
                        // rowid
                        byte[] bytes = Bytes.toBytes((short)0);
                        this.qualifiers[i] = bytes;
                    }
                    continue;
                }
                byte[] bytes = Bytes.toBytes(column.getColumnName());
                this.qualifiers[i] = bytes;
            }
        }
    }
    
    byte[] getSystemFamily() {
        return SYSTEM_FAMILY;
    }
    
    byte[] getUserFamily() {
        return USER_FAMILY;
    }
    
    byte[] getColumn(int i) {
        if (this.qualifiers != null) {
            return this.qualifiers[i];
        }
        else {
            return SYSTEM_QUALIFIERS[i];
        }
    }
    
    TableName getTableName() {
        return TableName.valueOf(this.ns, this.tableName);
    }
}
