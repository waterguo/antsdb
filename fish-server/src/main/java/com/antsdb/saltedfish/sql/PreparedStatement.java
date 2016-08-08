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
package com.antsdb.saltedfish.sql;

import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.Script;
import com.antsdb.saltedfish.sql.vdm.VdmContext;

/**
 * 
 * @author wgu0
 */
public class PreparedStatement {
	Script script;
	String sql;
	long version;
	
	PreparedStatement(Session session, String sql) {
		this.sql = sql;
		parse(session);
	}
	
    public Object run(Session session, Parameters params) {
    	while (isExpired(session)) {
    		parse(session);
    	}
    	VdmContext ctx = new VdmContext(session, this.script.getVariableCount());
		return this.script.run(ctx, params, 0);
	}

	private boolean isExpired(Session session) {
		boolean result = session.getOrca().getMetaService().getVersion() > this.version;
		return result;
	}
	
	private void parse(Session session) {
		this.version = session.getOrca().getMetaService().getVersion();
		this.script = session.parse(this.sql);
	}

	public String getSql() {
		return this.sql;
	}

	public int getParameterCount() {
		return this.script.getParameterCount();
	}

	public CursorMeta getCursorMeta() {
		return this.script.getCursorMeta();
	}
}
