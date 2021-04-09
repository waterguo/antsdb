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

import com.antsdb.saltedfish.nosql.Replicable;
import com.antsdb.saltedfish.nosql.Replicator;
import com.antsdb.saltedfish.server.mysql.ErrorMessage;

/**
 * 
 * @author *-xguo0<@
 */
public class StartReplicator extends Statement {

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        Replicator<Replicable> rep = ctx.getOrca().getReplicator();
        if (rep == null) {
            throw new ErrorMessage(0, "Replicator is not found");
        }
        if (ctx.getOrca().isSlave()) {
            throw new ErrorMessage(0, "Cant start replicator in slave mode");
        }
        rep.pause(false);
        return null;
    }

}
