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
package com.antsdb.saltedfish.storage;

import com.antsdb.saltedfish.sql.OrcaException;

/**
 * represents exceptions from hbase connector
 * 
 * @author wgu0
 */
public class OrcaHBaseException extends OrcaException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


    public OrcaHBaseException(Exception x, String message, Object... params) {
        super(x, message, params);
    }
    
	public OrcaHBaseException(String message, Object... params) {
		super(message, params);
	}

	public OrcaHBaseException(String message) {
	    super(message);
	}
	
	public OrcaHBaseException(Exception x) {
		super(x);
	}

}
