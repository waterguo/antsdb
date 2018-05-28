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

import java.util.BitSet;

import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.server.mysql.packet.PacketType;

/**
 * 
 * @author *-xguo0<@
 */
public class PacketExecute extends Packet {

    private int paramCount = -1;
    private int offsetHasTypes;
    private int offsetTypes;
    
    public PacketExecute(long addr, int length) {
        super(addr, length, PacketType.COM_STMT_EXECUTE);
    }

    public int getStatementId() {
        return Unsafe.getInt(this.addr + 4 + 1);
    }
    
    public int getFlags() {
        return Unsafe.getByte(this.addr + 4 + 1 + 4) & 0xff;
    }
    
    public Integer getType(int n) {
        if (this.offsetTypes == 0) {
            return null;
        }
        int result = Unsafe.getByte(this.addr + this.offsetTypes + n * 2) & 0xff;
        return result;
    }
    
    public Boolean isTypeUnsigned(int n) {
        if (this.offsetTypes == 0) {
            return null;
        }
        int result = Unsafe.getByte(this.offsetTypes + n * 2 + 1) & 0xff;
        return result == 128;
    }

    public boolean hasTypes() {
        int result = Unsafe.getByte(this.addr + this.offsetHasTypes);
        return result == 1;
    }
    
    public Boolean isNull(int idx) {
        if (this.paramCount < 0) {
            return null;
        }
        byte[] bytes = new byte[(this.paramCount + 7) / 8];
        Unsafe.getBytes(this.addr + 14, bytes);
        BitSet bits = BitSet.valueOf(bytes);
        return bits.get(idx);
    }
    
    @Override
    public String diffDump(int level) {
        return null;
    }
}
