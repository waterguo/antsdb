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
package com.antsdb.saltedfish;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.antsdb.saltedfish.util.UberUtil;

/*-------------------------------------------------------------------------------------------------
 _______ __   _ _______ _______ ______  ______ 
 |_____| | \  |    |    |______ |     \ |_____]
 |     | |  \_|    |    ______| |_____/ |_____]
 
 Copyright (c) 2016, AntsDB and/or its affiliates. All rights reserved.
 
 ------------------------------------------------------------------------------------------------*/

/**
 * 
 * @author wgu0
 */
public class TestLog4jMain {
	static Logger _log = UberUtil.getThisLogger();
	
	public static void main(String[] args) {
		_log.error("error");
		_log.warn("warn");
		_log.info("info");
		_log.debug("debug");
		_log.trace("trace");
		LoggerFactory.getLogger("com.antsdb.saltedfish.server.mysql.replication").info("replication info");
		LoggerFactory.getLogger("com.antsdb.saltedfish.server.mysql.replication").trace("replication trace");
	}

}
