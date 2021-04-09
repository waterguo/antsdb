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
package com.antsdb.saltedfish.obs;

import com.antsdb.saltedfish.nosql.StorageEngine;
import com.antsdb.saltedfish.parquet.ObsService;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;

public class StartObsBackup extends Instruction {
    
    private String dest;
    
    public StartObsBackup(String dest) {
        this.dest = dest;
    }
    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        StorageEngine stor = ctx.getHumpback().getStorageEngine0();
        if (stor instanceof ObsService) {
            ObsService service = (ObsService) stor;
            service.startBackup(this.dest);
        }
        return null;
    }
}
