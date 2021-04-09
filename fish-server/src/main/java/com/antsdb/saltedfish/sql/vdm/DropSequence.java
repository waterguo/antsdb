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

import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.SequenceMeta;

public class DropSequence extends Statement {
    ObjectName name;
    private boolean ifExist;
    
    public DropSequence(ObjectName name) {
        this(name, false);
    }
    
    public DropSequence(ObjectName name, boolean ifExist) {
        super();
        this.name = name;
        this.ifExist = ifExist;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        MetadataService metaService = ctx.getOrca().getMetaService();
        SequenceMeta seq = metaService.getSequence(ctx.getTransaction(), name);
        if (seq != null) {
            metaService.dropSequence(ctx.getHSession(), ctx.getTransaction(), seq);
        }
        else if (!ifExist) {
            throw new OrcaException("sequence not found: " + this.name);
        }
        return null;
    }

}
