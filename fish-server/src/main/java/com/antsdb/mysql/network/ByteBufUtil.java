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

import com.antsdb.saltedfish.util.BytesUtil;

import io.netty.buffer.ByteBuf;

/**
 * 
 * @author wgu0
 */
public final class ByteBufUtil {
	public static String dump(ByteBuf packet) {
		byte[] bytes = new byte[packet.readableBytes()];
		packet.getBytes(packet.readerIndex(), bytes);
		String dump = "\n" + BytesUtil.toHex(bytes);
		return dump;
	}
}
