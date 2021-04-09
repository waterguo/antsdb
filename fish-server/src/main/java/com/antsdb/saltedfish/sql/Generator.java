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
package com.antsdb.saltedfish.sql;

import org.antlr.v4.runtime.tree.ParseTree;

import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.Instruction;
import com.antsdb.saltedfish.sql.vdm.ObjectName;

public abstract class Generator<T extends ParseTree> {
    public abstract Instruction gen(GeneratorContext ctx, T rule) throws OrcaException;

    public boolean isTemporaryTable(GeneratorContext ctx, T rule) {
        return false;
    }
    
    protected static boolean isTemporaryTable(GeneratorContext ctx, ObjectName name) {
        TableMeta table = ctx.getSession().getTable(name);
        return table != null ? table.isTemproray() : false;
    }
}
