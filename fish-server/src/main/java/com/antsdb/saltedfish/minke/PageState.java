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
package com.antsdb.saltedfish.minke;

/**
 * 
 * @author *-xguo0<@
 */
public class PageState {
    public static final int FREE = 0;
    public static final int ACTIVE = 1;
    public static final int CARBONFREEZED = 2;
    public static final int GARBAGE = 3;
    
    public static String getStateName(int value) {
        String result = "";
        switch (value) {
        case FREE:
            result = "FREE";
            break;
        case ACTIVE:
            result = "ACTIVE";
            break;
        case CARBONFREEZED:
            result = "CBFD";
            break;
        case GARBAGE:
            result = "GARBAGE";
            break;
        }
        return result;
    }
}
