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

	@Override
	public int size() {
		return this.row.getSize();
	}

}
