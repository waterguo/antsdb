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
package com.antsdb.saltedfish.sql.vdm;

import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.util.CursorUtil;

public class ShowCharset extends CursorMaker {
    CursorMeta meta = new CursorMeta();

    public ShowCharset() {
        meta.addColumn(new FieldMeta("Charset", DataType.varchar()))
            .addColumn(new FieldMeta("Description", DataType.varchar()))
            .addColumn(new FieldMeta("Default collation", DataType.varchar()))
            .addColumn(new FieldMeta("Maxlen", DataType.integer()));
    }

    @Override
    public CursorMeta getCursorMeta() {
        return this.meta;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        // wgu0: hard coded for now
        Record rec = new HashMapRecord();
        rec.set(0, "ascii")
           .set(1, "US ASCII")
           .set(2, "ascii_general_ci")
           .set(3, 1);
        Cursor c = CursorUtil.toCursor(meta, rec);
        return c;
    }
}
