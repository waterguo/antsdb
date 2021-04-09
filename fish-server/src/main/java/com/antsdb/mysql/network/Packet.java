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

import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.server.fishnet.PacketFishBackup;
import com.antsdb.saltedfish.server.fishnet.PacketFishRestore;
import com.antsdb.saltedfish.server.mysql.packet.PacketType;
import com.antsdb.saltedfish.util.UberUtil;

import io.netty.buffer.ByteBuf;

/**
 * 
 * @author wgu0
 */
public abstract class Packet {
    protected long addr;
    protected int length;
    protected PacketType type;
    
    public Packet(long addr, int length, PacketType type) {
        this.addr = addr;
        this.length = length;
        this.type = type;
    }
    
    public int getSize() {
        return this.length;
    }
    
    public PacketType getType() {
        return this.type;
    }
    
    public int getSequence() {
        int sequence = Unsafe.getByte(this.addr + 3) & 0xff;
        return sequence;
    }
    
    public static Packet createPacket(PacketType type, ByteBuf buf) {
        long addr = buf.memoryAddress();
        int length = buf.readableBytes();
        return createPacket(buf.nioBuffer(), type, addr, length);
    }

    public static Packet createPacket(PacketType type, ByteBuffer buf) {
        long addr = UberUtil.getAddress(buf);
        int length = buf.remaining();
        return createPacket(buf, type, addr, length);
    }
    
    private static Packet createPacket(ByteBuffer buf, PacketType type, long addr, int length) {
        if (type == PacketType.COM_QUERY) {
            return new PacketQuery(buf);
        }
        else if (type == PacketType.FISH_RESULT_SET_COLUMN) {
            return new PacketResultSetColumn(addr, length);
        }
        else if (type == PacketType.FISH_HANDSHAKE) {
            return new PacketHandshake(addr, length);
        }
        else if (type == PacketType.FISH_AUTH) {
            return new PacketAuth41(addr, length);
        }
        else if (type == PacketType.FISH_OK) {
            return new PacketOk(addr, length);
        }
        else if (type == PacketType.FISH_EOF) {
            return new PacketEof(addr, length);
        }
        else if (type == PacketType.FISH_PREPARE_OK) {
            return new PacketPrepareOk(addr, length);
        }
        else if (type == PacketType.FISH_RESULT_SET_HEADER) {
            return new PacketResultSetHeader(addr, length);
        }
        else if (type == PacketType.FISH_RESULT_SET_ROW) {
            return new PacketResultSetRow(addr, length);
        }
        else if (type == PacketType.COM_PING) {
            return new PacketPing(addr, length);
        }
        else if (type == PacketType.COM_INIT_DB) {
            return new PacketDbInit(addr, length);
        }
        else if (type == PacketType.COM_QUIT) {
            return new PacketQuit(addr, length);
        }
        else if (type == PacketType.COM_STMT_PREPARE) {
            return new PacketPrepare(addr, length);
        }
        else if (type == PacketType.COM_STMT_CLOSE) {
            return new PacketStmtClose(addr, length);
        }
        else if (type == PacketType.COM_STMT_EXECUTE) {
            return new PacketExecute(addr, length);
        }
        else if (type == PacketType.COM_FIELD_LIST) {
            return new PacketFieldList(addr, length);
        }
        else if (type == PacketType.COM_SET_OPTION) {
            return new PacketSetOption(addr, length);
        }
        else if (type == PacketType.COM_STMT_SEND_LONG_DATA) {
            return new PacketLongData(addr, length);
        }
        else if (type == PacketType.FISH_BACKUP) {
            return new PacketFishBackup(addr, length);
        }
        else if (type == PacketType.FISH_RESTORE_START) {
            return new PacketFishRestoreStart(addr, length);
        }
        else if (type == PacketType.FISH_RESTORE) {
            return new PacketFishRestore(addr, length);
        }
        else if (type == PacketType.FISH_RESTORE_END) {
            return new PacketFishRestoreEnd(addr, length);
        }
        else if (type == PacketType.FISH_LOG_REPLICATE) {
            return new PacketLogReplicate(addr, length);
        }
        else {
            return new PacketUnknown(addr, length);
        }
    }

    public abstract String diffDump(int level);

    @Override
    public String toString() {
        return this.diffDump(0);
    }

    public static Packet from(ByteBuffer buf) {
        if (buf.position() != 0) {
            throw new IllegalArgumentException();
        }
        long addr = UberUtil.getAddress(buf) + buf.position();
        int size = buf.limit() - 4;
        byte cmd = PacketUtil.readByte(addr + 4);
        return createPacket(buf, PacketType.valueOf(cmd & 0xff), addr, size);
    }

    public long getAddress() {
        return this.addr;
    }
}
