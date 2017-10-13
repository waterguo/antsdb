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
 * a concurrent scalable data structure
 * 
 * @author *-xguo0<@
 */
public abstract class ScalableData {
	private volatile int ticket;
	
	/**
	 * grow the current storage 
	 * 
	 * @param filled the storage object that is full 
	 * @return
	 */
	protected synchronized void grow(int requestTicket) {
		if (this.ticket != requestTicket) {
			// caller should try again because a new storage object is created
			return;
		}
		int next = this.ticket + 1;
		extend(requestTicket, next);
		this.ticket = next;
	}
	
	/**
	 * Overridden by subclass to extend the storage object. this method will be called in a single thread context
	 * 
	 * @param current the current filled up storage object
	 * @return new object that extends the filled up one
	 */
	protected abstract void extend(int requestTicket, int nextTicket);
}
