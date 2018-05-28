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

import static com.antsdb.saltedfish.server.mysql.MysqlConstant.*;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * @see https://dev.mysql.com/doc/internals/en/connection-phase-packets.html
 * @author *-xguo0<@
 */
public class PacketAuth41 extends Packet {
	long addrPassword;
	long addrDatabase;
	long addrPluginAuth;
	
	public PacketAuth41(ByteBuffer packet) {
		this(UberUtil.getAddress(packet), packet.remaining());
	}

	public PacketAuth41(long addr, int length) {
	    super(addr, length, PacketType.FISH_AUTH);
		this.addrPassword = this.addr + 36 + getUser().length() + 1;
		this.addrDatabase = this.addrPassword + getPasswordBytes();
		this.addrPluginAuth = this.addrDatabase + getDatabaseBytes();	
	}

	private int getCapabilities() {
		return PacketUtil.readInt(this.addr + 4);
	}
	
	@Override
	public String diffDump(int level) {
		StringBuilder buf = new StringBuilder();
		buf.append(String.format(" capabilities=%08x\n", getCapabilities()));
		buf.append(String.format(" max_packet_size=%08x\n", getMaxPacketSize()));
		buf.append(String.format(" char_set=%d\n", getCharSet()));
		buf.append(String.format(" user_name=%s\n", getUser()));
		buf.append(String.format(" database=%s\n", getDatabase()));
		buf.append(String.format(" password=%s", getPassword()));
		return buf.toString();
	}

	private Object getPassword() {
		return null;
	}

	public Object getMaxPacketSize() {
		return PacketUtil.readInt(addr+8);
	}

	public byte getCharSet() {
		return Unsafe.getByte(addr+12);
	}

	public String getUser() {
		return PacketUtil.readString(this.addr+36, this.length, Decoder.UTF8);
	}

	private int getDatabaseBytes() {
		if ((getCapabilities() & CLIENT_CONNECT_WITH_DB) == 0) {
			return 0;
		}
		return getDatabase().length() + 1;
	}

	public String getDatabase() {
		if ((getCapabilities() & CLIENT_CONNECT_WITH_DB) == 0) {
			return null;
		}
		return PacketUtil.readString(this.addrDatabase, this.length, Decoder.UTF8);
	}
	
	private int getPasswordBytes() {
		if ((getCapabilities() & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
			return Unsafe.getByte(addrPassword) + 1;
		}
		else if ((getCapabilities() & CLIENT_SECURE_CONNECTION) != 0) {
			return Unsafe.getByte(addrPassword) + 1;
		}
		return 0;
	}
}
