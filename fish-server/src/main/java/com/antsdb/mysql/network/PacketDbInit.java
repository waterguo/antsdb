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

import com.antsdb.saltedfish.charset.Decoder;
import com.antsdb.saltedfish.server.mysql.packet.PacketType;

/**
 * 
 * @author *-xguo0<@
 */
public class PacketDbInit extends Packet {

    public PacketDbInit(long addr, int length) {
        super(addr, length, PacketType.COM_INIT_DB);
    }

    public String getDbName() {
        return PacketUtil.readString(addr + 5, this.length - 1, Decoder.UTF8);
    }
    
    @Override
    public String diffDump(int level) {
        return null;
    }

}
