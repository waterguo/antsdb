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

import java.nio.CharBuffer;

import com.antsdb.saltedfish.server.mysql.packet.PacketType;

/**
 * 
 * @author *-xguo0<@
 */
public class PacketPrepare extends Packet {
    
    PacketPrepare(long addr, int length) {
        super(addr, length, PacketType.COM_STMT_PREPARE);
    }
    
    public String getQuery() {
        return getQueryAsCharBuf().toString();
    }
    
    public CharBuffer getQueryAsCharBuf() {
        CharBuffer result = PacketUtil.readStringAsCharBufWithMysqlExtension(addr + 5, this.length - 1);
        result.flip();
        return result;
    }
    
    @Override
    public String diffDump(int level) {
        return getQuery();
    }

}
