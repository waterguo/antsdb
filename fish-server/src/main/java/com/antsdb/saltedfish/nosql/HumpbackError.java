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

/**
 * error code from humpback layer
 * 
 * @author wgu0
 */
public interface HumpbackError {
    /** operation is completed without logging activities */
    public static int NONE = 0;
    /** the record is locked by another pending trx */
    public static int LOCK_COMPETITION = 1;
    /** the record is updated/deleted by a concurrent trx */
    public static int CONCURRENT_UPDATE = 2;
    /** the record is missing for update/delete/lock operation */
    public static int MISSING = 3;
    /** the record already exists for insert operation */
    public static int EXISTS = 4;
    
    public static boolean isSuccess(long error) {
        return (error <= 0) || (error > 10);
    }
    
    public static String toString(long error) {
        if (error == LOCK_COMPETITION) {
            return "LOCK_COMPETITION: {}";
        }
        else if (error == CONCURRENT_UPDATE) {
            return "CONCURRENT_UPDATE: {}";
        }
        else if (error == MISSING) {
            return "MISSING: {}";
        }
        else if (error == EXISTS) {
            return "EXISTS: {}";
        }
        else {
            return "HumpbackError " + error + ": {}"; 
        }
    }
}
