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
package com.antsdb.saltedfish.sql.planner;

import java.util.List;

import com.antsdb.saltedfish.nosql.Statistician;
import com.antsdb.saltedfish.sql.meta.RuleMeta;

/**
 * 
 * @author *-xguo0<@
 */
class PathSeek extends Path {

    public PathSeek(RuleMeta<?> key, QueryNode node, List<ColumnFilter> consumed) {
        super(node);
    }

    @Override
    double getScore(Statistician stats) {
        return 1;
    }

}
