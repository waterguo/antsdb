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

import java.sql.SQLException;

import com.antsdb.saltedfish.sql.OrcaException;

/**
 * 
 * @author wgu0
 */
public class RunScript extends Instruction {
	String script;
	Parameters params; 
	
	public RunScript(String script) {
		super();
		this.script = script;
	}

	public RunScript(String sql, Object[] objects) {
		this.script = sql;
		params = new Parameters(objects);
	}

	@Override
	public Object run(VdmContext ctx, Parameters params, long pMaster) {
		try {
			return ctx.getSession().run(this.script, this.params);
		}
		catch (SQLException e) {
			throw new OrcaException(e);
		}
	}

}
