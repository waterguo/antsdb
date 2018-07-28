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
package com.antsdb.saltedfish.storage;

import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import com.antsdb.saltedfish.nosql.HColumnRow;
import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.sql.Orca;

/**
 * 
 * @author *-xguo0<@
 */
class Mapping {
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
    
    Mapping(String sysns, SysMetaRow tableInfo, List<HColumnRow> columns) {
        this.tableId = tableInfo.getTableId();
        this.ns = tableInfo.getNamespace();
        this.ns = (this.ns.equals(Orca.SYSNS)) ? sysns : this.ns; 
        this.tableName = tableInfo.getTableName();
        if ((columns != null) && (columns.size() > 0)) {
            int max = 0;
            for (HColumnRow i:columns) {
                if (i.isDeleted()) {
                    continue;
                }
                max = Math.max(max, i.getColumnPos());
            }
            this.qualifiers = new byte[max+1][];
            this.qualifiers[0] = Helper.SYS_COLUMN_ROWID_BYTES;
            for (HColumnRow i:columns) {
                if (i.isDeleted()) {
                    continue;
                }
                this.qualifiers[i.getColumnPos()] = Bytes.toBytes(i.getColumnName());
            }
        }
    }
    
    byte[] getUserFamily() {
        return USER_FAMILY;
    }
    
    byte[] getColumn(int i) {
        if (this.qualifiers != null) {
            return (i<this.qualifiers.length) ? this.qualifiers[i] : null;
        }
        else {
            return SYSTEM_QUALIFIERS[i];
        }
    }
    
    TableName getTableName() {
        return TableName.valueOf(this.ns, this.tableName);
    }
}
