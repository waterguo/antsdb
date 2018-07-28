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
package com.antsdb.saltedfish.sql;

import org.slf4j.helpers.MessageFormatter;

import com.antsdb.saltedfish.nosql.HumpbackError;

public class OrcaException extends RuntimeException {
    int code;
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public OrcaException() {
        super();
    }

    public OrcaException(HumpbackError error) {
        super(error.toString());
    }

    public OrcaException(HumpbackError error, Object... params) {
        super(MessageFormatter.arrayFormat(error.toString() + " {}", params).getMessage());
    }

    public OrcaException(String message) {
            super(message);
    }
    
    public OrcaException(Exception x, String message, Object... params) {
        super(MessageFormatter.arrayFormat(message, params).getMessage(), x);
    }

    public OrcaException(String message, Object... params) {
        super(MessageFormatter.arrayFormat(message, params).getMessage());
    }

    public OrcaException(int code, String message, Object... params) {
        super(message);
        this.code = code;
    }

    public OrcaException(Exception x) {
        super(x);
    }

}