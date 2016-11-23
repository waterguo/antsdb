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

import java.sql.Date;
import java.time.LocalDate;

import com.antsdb.saltedfish.cpp.FishDate;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;

/**
 * 
 * @author *-xguo0<@
 */
public class FuncCurDate extends Function {
    @SuppressWarnings("deprecation")
	@Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
    	LocalDate today = LocalDate.now();
    	Date date = new Date(today.getYear()-1900, today.getMonthValue()-1, today.getDayOfMonth());
    	return FishDate.allocSet(heap, date.getTime());
    }

    @Override
    public DataType getReturnType() {
        return DataType.date();
    }

	@Override
	public int getMinParameters() {
		return 0;
	}
}
