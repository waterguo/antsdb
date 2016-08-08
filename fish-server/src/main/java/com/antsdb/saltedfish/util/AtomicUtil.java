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
package com.antsdb.saltedfish.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 
 * @author wgu0
 */
public final class AtomicUtil {
	public static boolean max(AtomicLong x, long y) {
		for (;;) {
			long value = x.get();
			if (y > value) {
				if (!x.compareAndSet(value, y)) {
					continue;
				}
				return true;
			}
			else {
				return false;
			}
		}
	}

	public static boolean min(AtomicLong x, long y) {
		for (;;) {
			long value = x.get();
			if (y < value) {
				if (!x.compareAndSet(value, y)) {
					continue;
				}
				return true;
			}
			else {
				return false;
			}
		}
	}
}
