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
package com.antsdb.saltedfish.nosql;

/**
 * 
 * @author *-xguo0<@
 */
public class SysNamespaceRow {
    final static int COLUMN_NAMESPACE = 1;

    SlowRow row;
    
    SysNamespaceRow(String name) {
        this.row = new SlowRow(name.toLowerCase());
        setNamespace(name);
    }
    
    public SysNamespaceRow(Row row) {
        this(SlowRow.from(row));
    }
    
    public SysNamespaceRow(SlowRow row) {
        this.row = row;
    }
    
    public String getNamespace() {
        return (String)this.row.get(COLUMN_NAMESPACE);
    }
    
    void setNamespace(String value) {
        this.row.set(COLUMN_NAMESPACE, value);
    }
}
