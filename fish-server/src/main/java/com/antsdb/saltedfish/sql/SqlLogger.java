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

import java.io.File;

import com.antsdb.saltedfish.nosql.ConfigService;
import com.antsdb.saltedfish.util.BinaryLogger;

/**
 * used to log statements from end user
 * 
 * @author *-xguo0<@
 */
public final class SqlLogger {
    
    private BinaryLogger logger;

    public SqlLogger(File home, ConfigService config) {
        if ("write".equalsIgnoreCase(config.getSqlLogOption())) {
            int fileSize = config.getSqlLogFileSize();
            int entrySize = config.getSqlLogMaxEntrySize();
            this.logger = new BinaryLogger(home, "sqltrace", fileSize, entrySize);
        }
    }
    
    public void logRead() {
        if (this.logger == null) return;
    }
    
    public void logWrite(Session session, String message, Object... args) {
        if (this.logger == null) return;
        this.logger.log(message, session.getId(), args);
    }
    
    public void logMessage(String message, Object... args) {
        if (this.logger == null) return;
        this.logger.log(message, args);
    }
}
