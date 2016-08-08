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

/**
 * high performance but low precision timer
 * 
 * @author wgu0
 */
public final class UberTimer {
	long start;
	int ms;
	
	public UberTimer(int ms) {
		this.start = UberTime.getTime();
		this.ms = ms;
	}
	
	public boolean isExpired() {
		boolean result = (UberTime.getTime() - this.start) >= ms;
		return result;
	}
	
	public int getTimeOut() {
		return this.ms;
	}
}
