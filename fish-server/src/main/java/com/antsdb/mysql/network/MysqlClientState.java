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

import org.slf4j.Logger;

import com.antsdb.saltedfish.server.mysql.packet.PacketType;
import com.antsdb.saltedfish.util.UberUtil;

import io.netty.buffer.ByteBuf;

/**
 * state machine of the client side of the mysql protocol
 * 
 * @author wgu0
 */
public class MysqlClientState {
	final static int MAX_PACKET_SIZE = MysqlServerState.MAX_PACKET_SIZE;
	static Logger _log = UberUtil.getThisLogger();
	
	boolean isHandshaken = false;
	int currentRequest;
	ByteBuf currentReceived;
	PacketType currentReceivedType;
	boolean isIdling = false;;
	int eofCount=0;

	private boolean isLargePacket;
	
	public void setHandshaken(boolean b) {
		this.isHandshaken = b;
	}
	
	public PacketType getPacketType(ByteBuf packet) {
		PacketType type = null;
		if (this.isLargePacket) {
			type = PacketType.VERY_LARGE_PACKET;
		}
		else if (this.currentReceived == packet) {
			type = this.currentReceivedType;
		}
		return type;
	}

	public void notifySend(ByteBuf packet) {
		if (this.currentReceivedType == PacketType.FISH_HANDSHAKE) {
			this.currentRequest = PacketType.FISH_AUTH.getId();
		}
		else {
			if (!this.isLargePacket) {
				int size = packet.readableBytes();
				if (size == MAX_PACKET_SIZE) {
					this.isLargePacket = true;
				}
				this.currentRequest = packet.getByte(4) & 0xff;
			}
			else {
				int size = packet.readableBytes();
				if (size != MAX_PACKET_SIZE) {
					this.isLargePacket = false;
				}
			}
		}
		this.currentReceivedType = null;
		this.isIdling = this.isLargePacket ? true : false;
		this.eofCount = 0;
	}

	public void notifyReceive(ByteBuf packet) {
		this.isLargePacket = false;
		PacketType lastType = this.currentReceivedType; 
		this.currentReceivedType = null;
		this.currentReceived = packet;
		if (!isHandshaken) {
			this.isHandshaken = true;
			this.currentReceivedType = PacketType.FISH_HANDSHAKE;
			this.isIdling = true;
			return;
		}
		if (this.currentRequest == PacketType.FISH_AUTH.getId()) {
			if (!isError(packet)) {
				this.currentReceivedType = PacketType.FISH_AUTH_OK;
			}
			else {
				this.currentReceivedType = PacketType.FISH_ERROR;
			}
			this.isIdling = true;
		}
		else if (this.currentRequest == PacketType.COM_INIT_DB.getId()) {
			if (!isError(packet)) {
				this.currentReceivedType = PacketType.FISH_AUTH_OK;
			}
			else {
				this.currentReceivedType = PacketType.FISH_ERROR;
			}
			this.isIdling = true;
		}
		else if (this.currentRequest == PacketType.COM_QUERY.getId()) {
			if (lastType == null) {
				if (isOk(packet)) {
					this.currentReceivedType = PacketType.FISH_OK;
					this.isIdling = true;
				}
				else if (isError(packet)) {
					this.currentReceivedType = PacketType.FISH_ERROR;
					this.isIdling = true;
				}
				else {
					this.currentReceivedType = PacketType.FISH_RESULT_SET_HEADER;
				}
				this.eofCount = 0;
			}
			else if (lastType == PacketType.FISH_RESULT_SET_HEADER) {
				this.currentReceivedType = PacketType.FISH_RESULT_SET_COLUMN;
			}
			else if (lastType == PacketType.FISH_RESULT_SET_COLUMN) {
				if (!isEof(packet)) {
					this.currentReceivedType = PacketType.FISH_RESULT_SET_COLUMN;
				}
				else {
					this.currentReceivedType = PacketType.FISH_EOF;
				}
			}
			else if (lastType == PacketType.FISH_EOF) {
				if (!isEof(packet)) {
					this.currentReceivedType = PacketType.FISH_RESULT_SET_ROW;
				}
				else {
					this.currentReceivedType = PacketType.FISH_EOF;
					this.isIdling = true;
				}
			}
			else if (lastType == PacketType.FISH_RESULT_SET_ROW) {
				if (!isEof(packet)) {
					this.currentReceivedType = PacketType.FISH_RESULT_SET_ROW;
				}
				else {
					this.currentReceivedType = PacketType.FISH_EOF;
					this.isIdling = true;
				}
			}
		}
		else if (this.currentRequest == PacketType.COM_PING.getId()) {
			this.currentReceivedType = PacketType.FISH_OK;
			this.isIdling = true;
		}
		else {
			_log.warn("unknown command: {}", this.currentRequest);
		}
	}

	private boolean isEof(ByteBuf packet) {
		boolean result = ((packet.getByte(4) & 0xff) == 0xfe);
		return result;
	}

	private boolean isError(ByteBuf packet) {
		boolean result = ((packet.getByte(4) & 0xff) == 0xff);
		return result;
	}

	public boolean isOk(ByteBuf packet) {
		boolean result = (packet.getByte(4) == 0);
		return result;
	}
	
	public boolean isIdling() {
		return this.isIdling;
	}

}
