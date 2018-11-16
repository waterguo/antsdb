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

import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.sql.OrcaException;

public class CreateNamespace extends Statement {
    String name;
    boolean ifNotExists;
    
    
    public CreateNamespace(String name, boolean ifNotExists) {
        super();
        this.name = name;
        this.ifNotExists = ifNotExists;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        Humpback humpback = ctx.getOrca().getHumpback();
        if (humpback.getNamespace(this.name) != null) {
            if (this.ifNotExists) {
                return null;
            }
            else {
                throw new OrcaException("namespace already exist: " + this.name);                
            }
        }
        ctx.getHumpback().createNamespace(ctx.getHSession(), this.name);
        return null;
    }

}
