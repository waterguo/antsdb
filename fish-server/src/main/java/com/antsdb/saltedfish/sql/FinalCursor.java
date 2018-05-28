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

import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.Script;

/**
 * 
 * @author *-xguo0<@
 */
public class FinalCursor extends Cursor {

    Script script;
    Parameters params;
    Cursor upstream;

    public FinalCursor(Script script, Parameters params, Cursor upstream) {
        super(upstream.getMetadata());
        this.script = script;
        this.params = params;
        this.upstream = upstream;
    }

    @Override
    public long next() {
        return this.upstream.next();
    }

    @Override
    public void close() {
        this.upstream.close();
    }

    @Override
    public String toString() {
        return "final";
    }

}
