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
package com.antsdb.saltedfish.server.mysql;

import java.nio.ByteBuffer;

/**
 * 
 * @author *-xguo0<@
 */
public abstract class BufferWriter {
    public abstract Object getWrapped();
    public abstract void writeBytes(byte[] bytes);
    public abstract void writeBytes(ByteBuffer bytes);
    public abstract int position();
    public abstract void position(int pos);
    public abstract void readBytes(int start, byte[] bytes);
    public abstract void writeLong(long value);
    public abstract void writeByte(byte value);
    public abstract void writeShort(short value);
    public abstract void writeInt(int value);
    public abstract void writeBytes(long pValue, int len);
}
