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
package com.antsdb.saltedfish.slave;

import com.antsdb.saltedfish.nosql.Humpback;

/**
 * 
 * @author *-xguo0<@
 */
public class SlaveReplicator extends JdbcReplicator {
    public static final String KEY_HOST = "humpback.slave.host";
    public static final String KEY_PORT = "humpback.slave.port";
    public static final String KEY_USER = "humpback.slave.user";
    public static final String KEY_PASSWORD = "humpback.slave.password";
    public static final String KEY_ENABLED = "humpback.slave.enabled";
    
    public SlaveReplicator(Humpback humpback) throws Exception {
        super(humpback, 
              humpback.getConfig(KEY_HOST),
              humpback.getConfig(KEY_PORT),
              humpback.getConfig(KEY_USER),
              humpback.getConfig(KEY_PASSWORD));
    }
}
