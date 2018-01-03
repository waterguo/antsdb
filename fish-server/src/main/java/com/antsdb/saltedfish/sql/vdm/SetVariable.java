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

import java.util.Collections;
import java.util.List;

import com.antsdb.saltedfish.sql.meta.TableMeta;

public class SetVariable extends Statement {
    Operator expr;
    String name;
    
    public SetVariable(String name, Operator expr) {
        super();
        this.expr = expr;
        this.name = name;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        Object val = Util.eval(ctx, this.expr, params, 0);
        ctx.getSession().setVariable(this.name, val);
        return null;
    }

    @Override
    List<TableMeta> getDependents() {
        return Collections.emptyList();
    }

}
