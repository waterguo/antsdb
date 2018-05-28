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

import java.util.List;

/**
 * 
 * @author wgu0
 */
public class Vector {
    private List<Operator> values; 
    private boolean inclusive;
    private boolean isNullable;
    
    public Vector(List<Operator> values, boolean inclusive, boolean isNullable) {
        this.values = values;
        this.inclusive = inclusive;
        this.isNullable = isNullable;
    }

    public List<Operator> getValues() {
        return values;
    }

    public boolean isInclusive() {
        return inclusive;
    }

    public boolean isNullable() {
        return isNullable;
    }
    
}
