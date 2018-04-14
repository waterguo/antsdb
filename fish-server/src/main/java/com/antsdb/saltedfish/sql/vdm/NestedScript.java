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

public class NestedScript extends Instruction {
    Script source;
    Object[] params;
    
    public NestedScript(Script source, Object[] params) {
        super();
        this.source = source;
        this.params = params;
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Parameters full = new Parameters(this.params);
        if (params != null) {
            for (Object i:params.values) {
                full.values.add(i);
            }
        }
        Object result = this.source.run(ctx, full, pMaster);
        return result;
    }
    
    
}
