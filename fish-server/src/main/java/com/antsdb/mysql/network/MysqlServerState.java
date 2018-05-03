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

import com.antsdb.saltedfish.server.mysql.packet.PacketType;

import io.netty.buffer.ByteBuf;

/**
 * state machine of the server side of the mysql protocol
 *  
 * @author wgu0
 */
public final class MysqlServerState {
	final static int MAX_PACKET_SIZE = 0x1000003;
	
	boolean isHandshaken = false;
	ByteBuf authPacket = null;
	boolean isLargePacket = false;
	
	public PacketType getPacketType(ByteBuf packet) {
		if (packet == authPacket) {
			return PacketType.FISH_AUTH;
		}

		if (isLargePacket) {
			int sequence = packet.getByte(3) & 0xff;
			if (sequence > 0) {
				return PacketType.VERY_LARGE_PACKET;
			}
		}
		PacketType type = PacketType.valueOf(packet.getByte(4) & 0xff);
		return type;
	}

	public void notifyReceive(ByteBuf packet) {
		if (!isHandshaken) {
			isHandshaken = true;
			this.authPacket = packet;
			return;
		}
		int size = packet.readableBytes();
		if (isLargePacket) {
			if (size != MAX_PACKET_SIZE) {
				this.isLargePacket = false;
			}
			return;
		}
		this.authPacket = null;
		if (size == MAX_PACKET_SIZE) {
			this.isLargePacket = true;
		}
	}
}
