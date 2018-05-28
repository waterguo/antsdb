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

import com.antsdb.saltedfish.nosql.VaporizingRow;

/**
 * 
 * @author wgu0
 */
public class FishParameters extends Parameters {

    private VaporizingRow row;

    public FishParameters(VaporizingRow row) {
        this.row = row;
    }

    public long getAddress(int idx) {
        long pValue = row.getFieldAddress(idx);
        return pValue;
    }

    public Parameters toParameters() {
        if (this.row == null) {
            return null;
        }
        Object[] result = new Object[this.row.getMaxColumnId()+1];
        for (int i=0; i<result.length; i++) {
            result[i] = row.get(i);
        }
        return new Parameters(result);
    }
    
    @Override
    public int size() {
        if (this.row == null) {
            return 0;
        }
        return (this.row == null) ? 0 : this.row.getMaxColumnId() + 1;
    }

}
