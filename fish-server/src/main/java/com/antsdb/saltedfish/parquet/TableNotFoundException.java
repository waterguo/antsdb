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
package com.antsdb.saltedfish.parquet;

import com.antsdb.saltedfish.sql.OrcaException;

/**
 * represents exceptions from hbase connector
 * 
 * @author Frank Li<lizc@tg-hd.com>
 */
public class TableNotFoundException extends OrcaException {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;


    public TableNotFoundException(Exception x, String message, Object... params) {
        super(x, message, params);
    }
    
    public TableNotFoundException(String message, Object... params) {
        super(message, params);
    }

    public TableNotFoundException(String message) {
        super(message);
    }
    
    public TableNotFoundException(Exception x) {
        super(x);
    }

}
