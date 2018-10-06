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
package com.antsdb.saltedfish.sql.mysql;

import java.math.BigDecimal;
import java.util.Collections;

import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class PROFILING extends View {
    private static final CursorMeta META = CursorUtil.toMeta(Line.class);

    public static class Line {
        public Integer QUERY_ID;
        public Integer SEQ;
        public String STATE;
        public BigDecimal DURATION;
        public BigDecimal CPU_USER;
        public BigDecimal CPU_SYSTEM;
        public Integer CONTEXT_VOLUNTARY;
        public Integer CONTEXT_INVOLUNTARY;
        public Integer BLOCK_OPS_IN;
        public Integer BLOCK_OPS_OUT;
        public Integer MESSAGES_SENT;
        public Integer MESSAGES_RECEIVED;
        public Integer PAGE_FAULTS_MAJOR;
        public Integer PAGE_FAULTS_MINOR;
        public Integer SWAPS;
        public String SOURCE_FUNCTION;
        public String SOURCE_FILE;
        public Integer SOURCE_LINE;
    }

    public PROFILING() {
        super(META);
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        return CursorUtil.toCursor(META, Collections.emptyList());
    }
}
