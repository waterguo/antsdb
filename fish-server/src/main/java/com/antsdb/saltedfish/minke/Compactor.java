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
package com.antsdb.saltedfish.minke;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * periodically scan pages with low usage and merge them 
 *  
 * @author *-xguo0<@
 */
public class Compactor implements Callable<Long> {
    private Minke minke;
    
    Compactor(Minke minke) {
        this.minke = minke;
    }
    
    @Override
    public Long call() throws IOException {
        compact();
        return null;
    }

    private void compact() throws IOException {
        int threshold = this.minke.pageSize / 10 * 4;
        for (MinkeTable table:this.minke.getTables()) {
            table.compact(threshold);
        }
    }
}
