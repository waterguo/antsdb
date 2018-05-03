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

import java.nio.ByteBuffer;

import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.server.mysql.packet.PacketType;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * @see https://dev.mysql.com/doc/internals/en/connection-phase-packets.html
 * @author *-xguo0<@
 */
public class PacketHandshake extends Packet {
	long addrConnectionId;

	public PacketHandshake(ByteBuffer packet) {
		this(UberUtil.getAddress(packet), packet.remaining());
	}

	public PacketHandshake(long addr, int length) {
        super(addr, length, PacketType.FISH_HANDSHAKE);
		this.addrConnectionId = this.addr + 4 + 1 + PacketUtil.readString(this.addr+4+1, this.length).length() + 1;
	}
	
	public byte getProtocolVersion() {
		return Unsafe.getByte(this.addr+4);
	}
	
	public String getServerVersion() {
		return PacketUtil.readString(this.addr+4+1, this.length);
	}
	
	public int getConnectionId() {
		return PacketUtil.readInt(this.addrConnectionId);		
	}
	
	public String getAuthPluginDataPart1() {
		return PacketUtil.readFxiedLengthString(this.addrConnectionId + 4);
	}
	
	public int getServerCapabilities() {
		int result = PacketUtil.readShort(this.addrConnectionId + 13) & 0xffff;
		if (isLongPacket()) {
			result = result | PacketUtil.readShort(this.addrConnectionId + 18) << 16;
		}
		return result;
	}
	
	boolean isLongPacket() {
		return (this.addrConnectionId + 13 + 5 + 2 - this.addr) <= this.length;
	}
	
	byte getCharSet() {
		if (isLongPacket()) {
			return Unsafe.getByte(this.addrConnectionId + 15);
		}
		else {
			return 0;
		}
	}
	
	short getStatusFlags() {
		if (isLongPacket()) {
			return PacketUtil.readShort(this.addrConnectionId + 16);
		}
		else {
			return 0;
		}
	}
	
	@Override
	public String toString() {
		return diffDump(1);
	}

	@Override
	public String diffDump(int level) {
		StringBuilder buf = new StringBuilder();
		buf.append(String.format("  version=%d\n", getProtocolVersion()));
		buf.append(String.format("  server_capabilities=%08x\n", getServerCapabilities()));
		buf.append(String.format("  char_set=%d\n", getCharSet()));
		buf.append(String.format("  status_flags=%04x\n", getStatusFlags()));
		buf.append(String.format("  server_version=%s\n", getServerVersion()));
		if (level >= 1) {
			buf.append(String.format("  connection_id=%d\n", getConnectionId()));
			buf.append(String.format("  auth-plugin-data-part-1=%s\n", getAuthPluginDataPart1()));
		}
		return buf.toString();
	}
}
