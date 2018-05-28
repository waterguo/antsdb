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

import com.antsdb.saltedfish.nosql.SlowRow;
import com.antsdb.saltedfish.sql.Orca;

public class PrimaryKeyMeta extends RuleMeta<PrimaryKeyMeta> {

    public PrimaryKeyMeta(Orca orca, TableMeta owner) {
        super(orca, Rule.PrimaryKey, owner.getId());
        setTableId(owner.getId());
        setName("PK_" + Long.toHexString(getId()));
    }
    
    protected PrimaryKeyMeta(SlowRow row) {
        super(row);
    }

    public int getId() {
        return (int)row.get(ColumnId.sysrule_id.getId());
    }

    @Override
    public PrimaryKeyMeta clone() {
        PrimaryKeyMeta result = new PrimaryKeyMeta(this.row.clone());
        return result;
    }

}
