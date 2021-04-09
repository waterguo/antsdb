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
package com.antsdb.mysql.network;

import com.antsdb.saltedfish.server.mysql.packet.PacketType;

/**
 * 
 * @author *-xguo0<@
 */
public class PacketLogReplicate extends Packet {

    public PacketLogReplicate(long addr, int length) {
        super(addr, length, PacketType.FISH_LOG_REPLICATE);
    }

    public long getLogPointer() {
        return PacketUtil.readLongLong(this.addr + 5);
    }
    
    public long getLogEntryPointer() {
        return this.addr + 13;
    }
    
    @Override
    public String diffDump(int level) {
        return null;
    }

}
