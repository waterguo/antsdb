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
package com.antsdb.saltedfish.server.mysql;

/**
 * 
 * @author *-xguo0<@
 */
public class ErrorMessage extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    public static final int ZK_NOT_ENABLED = 50000;
    public static final int ZK_NODE_ALREADY_REGISTERED = 50001;
    public static final int ORCA_CANT_START_REPLICATOR = 20000;
    
    private int error;
    private String message;
    private String state;

    public ErrorMessage(int error, String message, Object... args) {
        this.error = error;
        this.message = String.format(message, args);
    }
    
    @Override
    public String getMessage() {
        return "#" + this.state + " " + message;
    }

    @Override
    public String toString() {
        return getMessage();
    }

    public int getError() {
        return this.error;
    }
}
