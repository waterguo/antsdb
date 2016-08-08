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
package com.antsdb.saltedfish.server.mysql.packet;

import io.netty.buffer.ByteBuf;

import java.io.UnsupportedEncodingException;

import com.antsdb.saltedfish.server.mysql.MysqlServerHandler;
import com.antsdb.saltedfish.server.mysql.util.BufferUtils;

/**
 * packet for switching database
 * 
 * @author wgu0
 */
public class InitPacket extends RecievePacket {
	public String database;
	
	public InitPacket(int command) {
		super(command);
	}

	@Override
	public void read(MysqlServerHandler handler, ByteBuf in) throws UnsupportedEncodingException {
		this.database = BufferUtils.readString(in);
	}

}
