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

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

/**
 * this is mapped to mysql SET NAMES statement
 * 
 * @author xinyi
 *
 */
public class SetNames extends Statement {
	static Logger _log = UberUtil.getThisLogger();
	
    String name;
    
    public SetNames(String name) {
        super();
        this.name = name;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
    	if (name.equals("latin1")) {
    		ctx.getSession().setProtocolCharset("latin1");
    	}
    	else if (name.equals("cp1250")) {
    		ctx.getSession().setProtocolCharset("cp1250");
    	}
    	else if (name.equals("utf8")) {
    		ctx.getSession().setProtocolCharset("utf8");
    	}
    	else if (name.equals("utf8mb4")) {
    		ctx.getSession().setProtocolCharset("utf8mb4");
    	}
    	else {
    		_log.warn("unknown codec: {}", this.name);
    	}
        return null;
    }

}
