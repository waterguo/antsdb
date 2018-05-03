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

import java.util.concurrent.atomic.AtomicLong;

import com.antsdb.saltedfish.server.mysql.packet.PacketType;
import com.antsdb.saltedfish.util.BytesUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class PacketResultSetRow extends Packet {
	
	public PacketResultSetRow(long addr, int length) {
        super(addr, length, PacketType.FISH_RESULT_SET_ROW);
	}

	@Override
	public String diffDump(int level) {
		StringBuilder buf = new StringBuilder();
		if (level >= 1) {
			int i = 0;
			long pEnd = this.addr + this.length;
			for (AtomicLong p = new AtomicLong(addr + 4); p.get() < pEnd; ) {
				buf.append(String.format("  field_%d=%s\n", i, getBytes(p)));
				i++;
			}
		}
		return buf.toString();
	}

	private String getBytes(AtomicLong p) {
		long length = PacketUtil.readLength(p.get());
		long pData = PacketUtil.skipLength(p.get());
		if (length == -1) {
			p.getAndIncrement();
			return "null";
		}
		length = Math.min(length, this.addr + this.length - p.get());
		p.set(pData + length);
		if (length >= 128) {
			length = 128;
		}
		return BytesUtil.toCompactHex(pData, (int)length);
	}

}
