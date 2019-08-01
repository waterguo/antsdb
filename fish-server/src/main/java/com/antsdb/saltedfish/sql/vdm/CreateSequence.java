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

public class CreateSequence extends Statement {
    ObjectName name;
    long seed;
    long increment;
    
    
    public CreateSequence(ObjectName name, long seed, long increment) {
        super();
        this.name = name;
        this.seed = seed;
        this.increment = increment;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        String ns = Checks.namespaceExist(ctx.getOrca(), name.getNamespace());
        Transaction trx = ctx.getTransaction();
        MetadataService metaService = ctx.getOrca().getMetaService();
        ObjectName canonizedName = new ObjectName(ns, this.name.getTableName());
        SequenceMeta seq = metaService.getSequence(trx, canonizedName);
        if (seq != null) {
            throw new OrcaException("sequence already exists: " + this.name);
        }
        seq = new SequenceMeta(ctx.getOrca(), canonizedName);
        seq.setNextNumber(this.seed);
        seq.setSeed(this.seed);
        seq.setIncrement(this.increment);
        metaService.addSequence(ctx.getHSession(), trx, seq);
        return null;
    }

}
