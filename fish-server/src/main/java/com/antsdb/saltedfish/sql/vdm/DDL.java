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

import java.util.List;

import com.antsdb.saltedfish.sql.meta.TableMeta;

/**
 * 
 * @author *-xguo0<@
 */
public class DDL extends Statement {
    
    private Instruction step;

    public DDL(String sql, Instruction step) {
        this.step = step;
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters params) {
        Object result = this.step.run(ctx, params, 0);
        return result;
    }

    @Override
    List<TableMeta> getDependents() {
        if (this.step instanceof Statement) {
            return ((Statement)step).getDependents();
        }
        else {
            return super.getDependents();
        }
    }

}
