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
package com.antsdb.saltedfish.sql.planner;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

/**
 * a single smallest query possible  
 *  
 * @author *-xguo0<@
 */
public class RowSet {
    public int tag;
    public List<ColumnFilter> conditions = new ArrayList<>();
    
    RowSet() {
    }
    
    RowSet(int tag) {
        this.tag = tag;
    }

    public void add(ColumnFilter cf) {
        this.conditions.add(cf);
    }

    @Override
    public String toString() {
        return StringUtils.join(this.conditions, ';');
    }
}
