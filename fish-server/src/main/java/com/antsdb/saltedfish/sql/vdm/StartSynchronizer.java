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

import com.antsdb.saltedfish.nosql.Synchronizer;

/**
 * 
 * @author *-xguo0<@
 */
public class StartSynchronizer extends Statement {

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        Synchronizer synchronzier = ctx.getHumpback().getSynchronizer();
        if (!synchronzier.isAlive()) {
            synchronzier.start();
        }
        return null;
    }

}
