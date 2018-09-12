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
package com.antsdb.saltedfish.nosql;

/**
 * 
 * @author *-xguo0<@
 */
public class SysConfigRow {
    final static int COLUMN_KEY = 1;
    final static int COLUMN_VALUE = 2;
    
    SlowRow row;

    public SysConfigRow(String key) {
        this.row = new SlowRow(key);
        setKey(key);
    }
    
    public SysConfigRow(SlowRow row) {
        super();
        this.row = row;
    }
    
    public String getKey() {
        return (String)this.row.get(COLUMN_KEY);
    }
    
    public void setKey(String value) {
        this.row.set(COLUMN_KEY, value);
    }
    
    public String getVale() {
        return (String)this.row.get(COLUMN_VALUE);
    }
    
    public void setValue(String value) {
        this.row.set(COLUMN_VALUE, value);
    }
}
