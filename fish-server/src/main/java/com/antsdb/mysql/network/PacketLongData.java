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
package com.antsdb.mysql.network;

import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.server.mysql.packet.PacketType;

/**
 * 
 * @author *-xguo0<@
 */
public class PacketLongData extends Packet {

    public PacketLongData(long addr, int length) {
        super(addr, length, PacketType.COM_STMT_SEND_LONG_DATA);
    }

    @Override
    public String diffDump(int level) {
        return null;
    }

    public int getStatementId() {
        return Unsafe.getInt(this.addr + 5);
    }
    
    public int getParameterId() {
        return Unsafe.getShort(this.addr + 9) & 0xffff;
    }
    
    public long getDataAddress() {
        return this.addr + 4 + 1 + 4 + 2;
    }

    public int getDataLength() {
        return this.length - 1 - 4 - 2;
    }
}
