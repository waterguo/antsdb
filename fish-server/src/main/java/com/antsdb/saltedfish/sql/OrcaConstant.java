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

import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.sql.meta.TableId;

public class OrcaConstant {
    public static final String SYSNS = Humpback.SYS_NAMESAPCE;
    
    public static final String TABLENAME_SYSSEQUENCE = "SYSSEQUENCE";
    public static final String TABLENAME_SYSTABLE = "SYSTABLE";
    public static final String TABLENAME_SYSCOLUMN = "SYSCOLUMN";
    public static final String TABLENAME_SYSPARAM = "SYSPARAM";
    public static final String TABLENAME_SYSRULE = "SYSRULE";
    public static final String TABLENAME_SYSUSER = "SYSUSER";
    
    public static final int TABLEID_SYSSEQUENCE = TableId.SYSSEQUENCE;
    public static final int TABLEID_SYSTABLE = TableId.SYSTABLE;
    public static final int TABLEID_SYSCOLUMN = TableId.SYSCOLUMN;
    public static final int TABLEID_SYSPARAM = TableId.SYSPARAM;
    public static final int TABLEID_SYSRULE = TableId.SYSRULE;
    public static final int TABLEID_SYSUSER = TableId.SYSUSER;
}
