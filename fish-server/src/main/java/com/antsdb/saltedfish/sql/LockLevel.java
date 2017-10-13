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
package com.antsdb.saltedfish.sql;

/**
 * 
 * @author wgu0
 */
public final class LockLevel {
	public static final int NONE = 0;
	public static final int SHARED = 1;
	public static final int EXCLUSIVE = 2;
	public static final int EXCLUSIVE_BY_OTHER = 3;
	
    public static String toString(int level) {
        switch (level) {
            case NONE:
                return "NONE";
            case SHARED:
                return "SHARED";
            case EXCLUSIVE:
                return "EXCLUSIVE";
            case EXCLUSIVE_BY_OTHER:
                return "EXCLUSIVE_BY_OTHER";
            default:
                return "UNKNOWN";
        }
    }
}
