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

import java.nio.ByteBuffer;

import com.antsdb.saltedfish.charset.Decoder;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.server.mysql.packet.PacketType;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class PacketError extends Packet {

    public PacketError(long addr, int length) {
        super(addr, length, PacketType.FISH_ERROR);
    }

    public PacketError(ByteBuffer buf) {
        this(UberUtil.getAddress(buf), buf.remaining());
    }

    @Override
    public String diffDump(int level) {
        StringBuilder buf = new StringBuilder();
        buf.append(String.format("  code=%d\n", getErrorCode()));
        buf.append(String.format("  sqlstate=%s\n", getSqlState()));
        buf.append(String.format("  message=%s\n", getErrorMessage()));
        return buf.toString();
    }

    public int getErrorCode() {
        return Unsafe.getShort(this.addr + 5) & 0xffff;
    }
    
    public String getSqlState() {
        byte[] bytes = new byte[6];
        Unsafe.getBytes(addr + 7, bytes);
        if (bytes[0] != '#') {
            return null;
        }
        return new String(bytes);
    }
    
    public String getErrorMessage() {
        if (getSqlState() == null) {
            return PacketUtil.readString(this.addr + 7, this.length - 7 + 4, Decoder.UTF8);
        }
        else {
            return PacketUtil.readString(this.addr + 13, this.length - 13 + 4, Decoder.UTF8);
        }
    }
}
