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
package com.antsdb.saltedfish.sql.meta;

public final class TableId {
    public static final int SYSSEQUENCE = 0x50;
    public static final int SYSTABLE = 0x51;
    public static final int SYSCOLUMN = 0x52;
    public static final int SYSPARAM = 0x53;
    public static final int SYSRULE = 0x54;
    public static final int SYSUSER = 0x55;
    public static final int MAX = 0xff;
            
    public static int valueOf(String name) {
        switch (name) {
        case "SYSSEQUENCE": return SYSSEQUENCE;
        case "SYSTABLE": return SYSTABLE;
        case "SYSCOLUMN": return SYSCOLUMN;
        case "SYSPARAM": return SYSPARAM;
        case "SYSRULE": return SYSRULE;
        default:
            throw new IllegalArgumentException(name);
        }
    }
}
