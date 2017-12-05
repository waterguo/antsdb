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

import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class ShowStatus extends ViewMaker {
    private static final CursorMeta META = CursorUtil.toMeta(Line.class);
    
    public static class Line {
        public String VARIABLE_NAME;
        public String VARIABLE_VALUE;
    }

    public ShowStatus() {
        super(META);
    }

    @Override
    public Object run(VdmContext ctx, Parameters notused, long pMaster) {
        return CursorUtil.toCursor(META, Collections.emptyList());
    }
}
