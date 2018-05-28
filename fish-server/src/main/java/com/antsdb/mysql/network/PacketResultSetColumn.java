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

import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.server.mysql.packet.PacketType;

import io.netty.buffer.ByteBuf;

/**
 * 
 * @author wgu0
 */
public class PacketResultSetColumn extends Packet {
	long pSchema;
	long pTable;
	long pTableAlias;
	long pColumnAlias;
	long pColumn;
	long pCharset;
	
	public PacketResultSetColumn(ByteBuf buf) {
		this(buf.memoryAddress(), buf.readableBytes());
	}
	
	public PacketResultSetColumn(long addr, int length) {
        super(addr, length, PacketType.FISH_RESULT_SET_COLUMN);
		this.pSchema = PacketUtil.skipStringWithLength(this.addr + 4);
		this.pTableAlias = PacketUtil.skipStringWithLength(this.pSchema);
		this.pTable = PacketUtil.skipStringWithLength(this.pTableAlias);
		this.pColumnAlias = PacketUtil.skipStringWithLength(this.pTable);
		this.pColumn = PacketUtil.skipStringWithLength(this.pColumnAlias);
		this.pCharset = PacketUtil.skipStringWithLength(this.pColumn) + 1;
	}

	@Override
	public String toString() {
		return this.diffDump(1);
	}
	
	public String getCatalog() {
		return PacketUtil.readStringWithLength(this.addr + 4);
	}

	public String getSchema() {
		return PacketUtil.readStringWithLength(this.pSchema);
	}
	
	public String getTableAlias() {
		return PacketUtil.readStringWithLength(this.pTableAlias);
	}

	public String getTable() {
		return PacketUtil.readStringWithLength(this.pTable);
	}

	public String getColumnAlias() {
		return PacketUtil.readStringWithLength(this.pColumnAlias);
	}

	public String getColumn() {
		return PacketUtil.readStringWithLength(this.pColumn);
	}
	
	public short getCharSet() {
		return Unsafe.getShort(pCharset);
	}
	
	public long getColumnLength() {
		return Unsafe.getInt(pCharset + 2);
	}

	public int getColumnType() {
		return Unsafe.getByte(pCharset + 6) & 0xff;
	}

	public short getFlags() {
		return Unsafe.getShort(pCharset + 7);
	}
	
	public long getScale() {
		return Unsafe.getByte(pCharset + 9);
	}

	@Override
	public String diffDump(int level) {
		StringBuilder buf = new StringBuilder();
		buf.append(String.format("  catalog=%s\n", getCatalog()));
		buf.append(String.format("  schema=%s\n", getSchema()));
		buf.append(String.format("  tableAlias=%s\n", getTableAlias()));
		buf.append(String.format("  table=%s\n", getTable()));
		buf.append(String.format("  columnAlias=%s\n", getColumnAlias()));
		buf.append(String.format("  column=%s\n", getColumn()));
		buf.append(String.format("  charset=%s\n", getCharSet()));
		buf.append(String.format("  type=%s\n", getColumnType()));
		if (level >= 1) {
			buf.append(String.format("  length=%s\n", getColumnLength()));
			buf.append(String.format("  scale=%s\n", getScale()));
		}
		return buf.toString();
	}

}
