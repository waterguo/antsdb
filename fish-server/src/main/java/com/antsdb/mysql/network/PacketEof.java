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

/**
 * 
 * @author *-xguo0<@
 */
public class PacketEof extends Packet {

	public PacketEof(long addr, int length) {
        super(addr, length, PacketType.FISH_EOF);
	}

	public int getWarningCount() {
		return PacketUtil.readShort(addr + 4 + 1);
	}
	
	public int getStatus() {
		return PacketUtil.readShort(addr + 4 + 3);
	}
	
	@Override
	public String diffDump(int level) {
		StringBuilder buf = new StringBuilder();
		buf.append(String.format(" status_flags=%04x", getStatus()));
		if (level >= 1) {
		    buf.append('\n');
			buf.append(String.format(" warnings=%d", getWarningCount()));
		}
		return buf.toString();
	}

}
