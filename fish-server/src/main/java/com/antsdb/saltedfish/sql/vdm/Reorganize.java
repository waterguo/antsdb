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

import java.io.IOException;

import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.Synchronizer;
import com.antsdb.saltedfish.sql.OrcaException;

/**
 * 
 * @author *-xguo0<@
 */
public class Reorganize extends Statement {
    @Override
    public Object run(VdmContext ctx, Parameters params) {
        // 1. freeze and carbonfreeze tablets
        
        carbonfreeze(ctx);
        
        // 2. minke synchronization
        
        synchronize(ctx);
        
        // 3. recycle
        
        ctx.getSession().notifyEndQuery();
        recycle(ctx);
        
        return null;
    }

    private void recycle(VdmContext ctx) {
        ctx.getOrca().recycle();
    }

    private void synchronize(VdmContext ctx) {
        Synchronizer sync = ctx.getHumpback().getSynchronizer();
        try {
            sync.sync(true);
        }
        catch (Exception x) {
            throw new OrcaException(x);
        }
    }

    private void carbonfreeze(VdmContext ctx) {
        long lastClosedTrxId = ctx.getOrca().getLastClosedTransactionId();
        Humpback humpback = ctx.getHumpback();
        try {
            humpback.carbonfreeze(lastClosedTrxId, true);
        }
        catch (IOException e) {
            throw new OrcaException(e);
        }
    }

}
