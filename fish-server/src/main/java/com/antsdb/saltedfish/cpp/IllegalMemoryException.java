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
package com.antsdb.saltedfish.cpp;

import org.slf4j.helpers.MessageFormatter;

import com.antsdb.saltedfish.util.UberFormatter;

/**
 * 
 * @author *-xguo0<@
 */
public class IllegalMemoryException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    long pMemory;
    
    public IllegalMemoryException(long pMemory) {
        super(dump(pMemory));
        this.pMemory = pMemory;
    }
    
    public IllegalMemoryException(long pMemory, String message, Object... params) {
        super(MessageFormatter.arrayFormat(message, params).getMessage() + "\n" + dump(pMemory));
    }

    private static String dump(long pMemory) {
        if (pMemory == 0) {
            return "null";
        }
        return UberFormatter.hex(pMemory);
    }

}
