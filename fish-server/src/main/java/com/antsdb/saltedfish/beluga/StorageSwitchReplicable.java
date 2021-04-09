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
package com.antsdb.saltedfish.beluga;

import com.antsdb.saltedfish.nosql.Replicable;
import com.antsdb.saltedfish.nosql.ReplicationHandler2;

/**
 * 
 * @author *-xguo0<@
 */
public class StorageSwitchReplicable implements Replicable {

    private Replicable upstream;
    private StorageSwitch owner;

    public StorageSwitchReplicable(StorageSwitch owner, Replicable upstream) {
        this.upstream = upstream;
        this.owner = owner;
    }

    @Override
    public long getReplicateLogPointer() {
        return this.upstream.getReplicateLogPointer();
    }

    @Override
    public long getCommittedLogPointer() {
        return this.upstream.getCommittedLogPointer();
    }

    @Override
    public ReplicationHandler2 getReplayHandler() {
        return new StorageSwitchReplayHandler(this.owner, this.upstream.getReplayHandler());
    }

    @Override
    public void connect() throws Exception {
        this.upstream.connect();
    }

    @Override
    public void setLogPointer(long value) {
        this.upstream.setLogPointer(value);
    }

}
