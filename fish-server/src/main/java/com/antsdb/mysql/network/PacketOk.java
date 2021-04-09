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
public class PacketOk extends Packet {
    long updateCount;
    long insertId;
    int status;
    int warningCount;
    long pMessage;
    
    public PacketOk(long addr, int length) {
        super(addr, length, PacketType.FISH_OK);
        this.updateCount = PacketUtil.readLength(addr+4+1);
        long pInsertId = PacketUtil.skipLength(addr+4+1);
        this.insertId = PacketUtil.readLength(pInsertId);
        long pStatus = PacketUtil.skipLength(pInsertId);
        this.status = PacketUtil.readShort(pStatus);
        this.warningCount = PacketUtil.readShort(pStatus + 2);
        this.pMessage = pStatus + 4;
    }

    public long getAffectedRows() {
        return this.updateCount;
    }
    
    public long getLastInsertId() {
        return this.insertId;
    }
    
    public int getStatus() {
        return this.status;
    }
    
    public int getWarningCount() {
        return this.warningCount;
    }
    
    public String getMessage() {
        return PacketUtil.readStringWithLength(this.pMessage);
    }
    
    @Override
    public String diffDump(int level) {
        StringBuilder buf = new StringBuilder();
        buf.append(String.format(" affected_rows=%d\n", getAffectedRows()));
        buf.append(String.format(" last_insert_id=%d\n", getLastInsertId()));
        buf.append(String.format(" status=0x%04x\n", getStatus()));
        if (level >= 1) {
            buf.append(String.format(" warnings=%d\n", getWarningCount()));
        }
        return buf.toString();
    }

    @Override
    public String toString() {
        return diffDump(0);
    }

}
