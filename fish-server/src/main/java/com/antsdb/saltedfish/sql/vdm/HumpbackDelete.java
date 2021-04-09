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

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.HumpbackError;
import com.antsdb.saltedfish.nosql.TrxMan;
import com.antsdb.saltedfish.sql.OrcaException;

/**
 * 
 * @author wgu0
 */
public class HumpbackDelete extends Instruction {
    int tableId;
    byte[] key;
    
    public HumpbackDelete(int tableId, byte[] key) {
        this.tableId = tableId;
        this.key = key;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        GTable table = ctx.getHumpback().getTable(this.tableId);
        if (table == null) {
            throw new OrcaException("table {} is not found", tableId);
        }
        long trxid = TrxMan.getNewVersion();
        int timeout = ctx.getSession().getConfig().getLockTimeout();
        long error = table.delete(ctx.getHSession(), trxid, key, timeout);
        if (!HumpbackError.isSuccess(error)) {
            throw new OrcaException(HumpbackError.toString(error));
        }
        return 1;
    }

}
