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
public class PacketPrepareOk extends Packet {

    public PacketPrepareOk(long addr, int length) {
        super(addr, length, PacketType.FISH_PREPARE_OK);
    }

    public int getStatementId() {
        return Unsafe.getInt(this.addr + 5);
    }
    
    public int getColumnCount() {
        return Unsafe.getShort(this.addr + 9);
    }
    
    public int getParameterCount() {
        return Unsafe.getShort(this.addr + 11);
    }
    
    public int getWarningCount() {
        return Unsafe.getShort(this.addr + 14);
    }
    
    @Override
    public String diffDump(int level) {
        StringBuilder buf = new StringBuilder();
        buf.append(String.format("statementId=%d", getStatementId()));
        buf.append('\n');
        buf.append(String.format("columnCount=%d", getColumnCount()));
        buf.append('\n');
        buf.append(String.format("parameterCount=%d", getParameterCount()));
        buf.append('\n');
        buf.append(String.format("warningCount=%d", getWarningCount()));
        buf.append('\n');
        return buf.toString();
    }

}
