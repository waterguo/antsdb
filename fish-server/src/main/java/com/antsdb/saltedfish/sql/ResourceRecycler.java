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

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class ResourceRecycler implements Runnable {
    static Logger _log = UberUtil.getThisLogger();
    
    private Orca orca;
    
    ResourceRecycler(Orca orca) {
        this.orca = orca;
    }
    
    @Override
    public void run() {
        try {
            run_();
        }
        catch (Exception x) {
            _log.error("error in resource recycler", x);
        }
    }

    public void run_() {
        synchronized(this.orca) {
            this.orca.recycle();
        }
    }

}
