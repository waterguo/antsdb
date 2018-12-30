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

import java.util.Collections;
import java.util.List;

import com.antsdb.saltedfish.slave.JdbcReplicator;

/**
 * 
 * @author *-xguo0<@
 */
class SlaveLogDependency implements LogDependency {

    private Humpback humpback;

    public SlaveLogDependency(Humpback humpback) {
        this.humpback = humpback;
    }

    @Override
    public String getName() {
        return "replicators";
    }

    @Override
    public List<LogDependency> getChildren() {
        if (this.humpback.getSlaveReplicator() == null) {
            return null;
        }
        Replicator<JdbcReplicator> slave = this.humpback.getSlaveReplicator();
        LogDependency result = new LogDependency() {
            @Override
            public String getName() {
                return slave.getName();
            }

            @Override
            public long getLogPointer() {
                return slave.getLogPointer();
            }
        };
        return Collections.singletonList(result);
    }

}
