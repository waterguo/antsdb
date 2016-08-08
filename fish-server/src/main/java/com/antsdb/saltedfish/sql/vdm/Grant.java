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
package com.antsdb.saltedfish.sql.vdm;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

public class Grant extends Statement {
    static Logger _log = UberUtil.getThisLogger();
    
    String userName;
    
    public Grant(String userName) {
        super();
        this.userName = userName;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        _log.warn("grant statement is ignored");
        return null;
    }

}
