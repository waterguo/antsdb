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

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;

import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.Script;
import com.antsdb.saltedfish.sql.vdm.VdmContext;

public abstract class SqlParserFactory {
    public abstract Script parse(Session session, CharStream cs);
    public abstract Object run(VdmContext ctx, Parameters params, CharStream cs);
    
    public Script parse(Session session, String text) {
        CharStream cs = new ANTLRInputStream(text);
        return parse(session, cs);
    }
    
    public Object run(VdmContext ctx, Parameters params, String sql) {
        CharStream cs = new ANTLRInputStream(sql);
        return run(ctx, params, cs);
    }
}
