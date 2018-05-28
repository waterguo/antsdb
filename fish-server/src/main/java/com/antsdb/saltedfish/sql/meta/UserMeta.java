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
package com.antsdb.saltedfish.sql.meta;

import java.util.Optional;

import com.antsdb.saltedfish.nosql.SlowRow;

/**
 * 
 * @author *-xguo0<@
 */
public class UserMeta {
    SlowRow row;

    public UserMeta(int id) {
        this.row = new SlowRow(id);
        setId(id);
    }
    
    public UserMeta(SlowRow row) {
        this.row = row;
    }

    public byte[] getPassword() {
        byte[] result = (byte[])this.row.get(ColumnId.sysuser_password.getId());
        return result;
    }

    public void setPassword(byte[] value) {
        this.row.set(ColumnId.sysuser_password.getId(), value);
    }

    public void setDeleteMark(boolean value) {
        this.row.set(ColumnId.sysuser_delete_mark.getId(), value);
    }

    public boolean isDeleted() {
        Boolean result = (Boolean)this.row.get(ColumnId.sysuser_delete_mark.getId());
        return Optional.ofNullable(result).orElseGet(()->{return false;});
    }

    public String getName() {
        return (String)this.row.get(ColumnId.sysuser_name.getId());
    }
    
    public void setName(String value) {
        this.row.set(ColumnId.sysuser_name.getId(), value);
    }

    public int getId() {
        return (int)this.row.get(ColumnId.sysuser_id.getId());
    }

    private void setId(int value) {
        this.row.set(ColumnId.sysuser_id.getId(), value);
    }

}
