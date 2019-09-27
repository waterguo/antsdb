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

public class SetSessionParameter extends Statement {
    String name;
    Operator op;
    
    
    public SetSessionParameter(String name, Operator op) {
        super();
        this.name = name;
        this.op = op;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        Object val = Util.eval(ctx, this.op, params, 0);
        try {
            ctx.getSession().getConfig().set(this.name, val == null ? null : val.toString());
        }
        catch (Exception x) {
            throw new OrcaException(x);
        }
        return null;
    }

}
