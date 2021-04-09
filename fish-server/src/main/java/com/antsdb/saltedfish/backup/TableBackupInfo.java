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
package com.antsdb.saltedfish.backup;

/**
 * 
 * @author *-xguo0<@
 */
public class TableBackupInfo {
    public String catalog;
    public String schema;
    public String table;
    public String create;
    public ColumnBackupInfo[] columns;
    
    public String getFullName() {
        String result = this.table;
        if (this.schema != null) result = this.schema + "." + result;
        if (this.catalog != null) result = this.catalog + "." + result;
        return result;
    }
}
