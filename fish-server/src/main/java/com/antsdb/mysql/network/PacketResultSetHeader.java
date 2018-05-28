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

import com.antsdb.saltedfish.server.mysql.packet.PacketType;

/**
 * 
 * @author *-xguo0<@
 */
public class PacketResultSetHeader extends Packet {
	long fieldCount;
	
	public PacketResultSetHeader(long addr, int length) {
        super(addr, length, PacketType.FISH_RESULT_SET_HEADER);
		this.fieldCount = PacketUtil.readLength(addr+4);
	}

	@Override
	public String diffDump(int level) {
		return String.format("  field_count=%d", this.fieldCount);
	}

}
