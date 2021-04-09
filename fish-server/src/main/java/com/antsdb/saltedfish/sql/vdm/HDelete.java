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

import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.HumpbackError;
import com.antsdb.saltedfish.nosql.TrxMan;
import com.antsdb.saltedfish.sql.OrcaException;

/**
 * 
 * @author *-xguo0<@
 */
public class HDelete extends Statement {

    private GTable gtable;
    private CursorMaker maker;

    public HDelete(GTable gtable, CursorMaker maker) {
        this.gtable = gtable;
        this.maker = maker;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        Cursor c = (Cursor)maker.run(ctx, params, 0);
        int count = 0;
        for (long pRecord = c.next();pRecord != 0;pRecord = c.next()) {
            long pKey = Record.getKey(pRecord);
            long version = TrxMan.getNewVersion();
            long error = this.gtable.delete(ctx.getHSession(), version, pKey, 0);
            if (!HumpbackError.isSuccess(error)) {
                throw new OrcaException("unable to delete row {} due to {}", KeyBytes.toString(pKey), error); 
            }
            count++;
        }
        return count;
    }

}
