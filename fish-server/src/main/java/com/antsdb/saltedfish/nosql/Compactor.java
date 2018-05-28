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
package com.antsdb.saltedfish.nosql;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

import io.netty.util.internal.ThreadLocalRandom;

/**
 * 
 * @author wgu0
 */
public class Compactor implements Runnable {
	static Logger _log = UberUtil.getThisLogger();
	
	private Humpback humpback;

	public Compactor(Humpback humpback) {
		this.humpback = humpback;
	}
	
	@Override
	public void run() {
		try {
			List<GTable> tables = new ArrayList<>(humpback.getTables());
			if (tables.size() == 0) {
				return;
			}
			int idx = ThreadLocalRandom.current().nextInt(0, tables.size());
			GTable gtable = tables.get(idx);
			gtable.getMemTable().compact();
		}
		catch (Exception x) {
			_log.error("", x);
		}
	}

}
