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

import com.antsdb.saltedfish.nosql.CheckPoint;

/**
 * 
 * @author wgu0
 */
public class ChangeMaster extends Instruction {
	String logfile;
	long logpos;
	
	@Override
	public Object run(VdmContext ctx, Parameters params, long pMaster) {
		CheckPoint cp = ctx.getOrca().getHumpback().getCheckPoint();
		cp.setSlaveLogFile(this.logfile);
		cp.setSlaveLogPosition(this.logpos);
		return null;
	}

	public void setLogFile(String value) {
		this.logfile = value;
	}
	
	public void setLogPos(long value) {
		this.logpos = value;
	}
}
