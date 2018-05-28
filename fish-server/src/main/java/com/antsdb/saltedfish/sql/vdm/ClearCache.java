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

import com.antsdb.saltedfish.minke.ClearEvictor;
import com.antsdb.saltedfish.minke.MinkeCache;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.StorageEngine;
import com.antsdb.saltedfish.sql.OrcaException;

/**
 * clear entire cache
 * 
 * @author *-xguo0<@
 */
public class ClearCache extends Statement {

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        Humpback humpback = ctx.getHumpback();
        StorageEngine stor = humpback.getStorageEngine();
        if (!(stor instanceof MinkeCache)) {
            throw new OrcaException("cache is not activiated in your configuration");
        }
        MinkeCache cache = (MinkeCache)stor;
        ClearEvictor evictor = new ClearEvictor(cache);
        evictor.run();
        ctx.getSession().notifyEndQuery();
        try {
            stor.checkpoint();
        }
        catch (Exception x) {
            throw new OrcaException(x);
        }
        ctx.getOrca().recycle();
        return null;
    }

}
