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

import java.nio.MappedByteBuffer;

/**
 * 
 * @author *-xguo0<@
 */
public class SpaceWarmer implements Runnable {

    private SpaceManager sm;
    byte checksum = 0;

    SpaceWarmer(SpaceManager sm) {
        this.sm = sm;
    }
    
    @Override
    public void run() {
        for (int i=this.sm.spaces.length-1; i>=0; i--) {
            Space ii = this.sm.spaces[i];
            if (ii != null) {
                MappedByteBuffer buf = ii.mmf.buf;
                for (int j=0; i<buf.capacity(); j+=0x100) {
                    this.checksum ^= buf.get(j);
                }
            }
        }
    }
}
