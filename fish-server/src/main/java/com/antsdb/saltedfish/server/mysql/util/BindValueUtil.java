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
package com.antsdb.saltedfish.server.mysql.util;

import io.netty.buffer.ByteBuf;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import com.antsdb.saltedfish.cpp.FishDate;
import com.antsdb.saltedfish.cpp.FishNumber;
import com.antsdb.saltedfish.cpp.FishTime;
import com.antsdb.saltedfish.cpp.FishTimestamp;
import com.antsdb.saltedfish.cpp.FishUtf8;
import com.antsdb.saltedfish.cpp.Float4;
import com.antsdb.saltedfish.cpp.Float8;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Int4;
import com.antsdb.saltedfish.cpp.Int8;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * @author roger
 */
public final class BindValueUtil {

    public static final void read(MySQLMessage mm, BindValue bv) throws UnsupportedEncodingException {
        switch (bv.type & 0xff) {
        case Fields.FIELD_TYPE_BIT:
            bv.value = mm.readBytesWithLength();
            break;
        case Fields.FIELD_TYPE_TINY:
            bv.byteBinding = mm.read();
            break;
        case Fields.FIELD_TYPE_SHORT:
            bv.shortBinding = (short) mm.readUB2();
            break;
        case Fields.FIELD_TYPE_LONG:
            bv.intBinding = mm.readInt();
            break;
        case Fields.FIELD_TYPE_LONGLONG:
            bv.longBinding = mm.readLong();
            break;
        case Fields.FIELD_TYPE_FLOAT:
            bv.floatBinding = mm.readFloat();
            break;
        case Fields.FIELD_TYPE_DOUBLE:
            bv.doubleBinding = mm.readDouble();
            break;
        case Fields.FIELD_TYPE_TIME:
            bv.value = mm.readTime();
            break;
        case Fields.FIELD_TYPE_DATE:
        case Fields.FIELD_TYPE_DATETIME:
        case Fields.FIELD_TYPE_TIMESTAMP:
            bv.value = mm.readDate();
            break;
        case Fields.FIELD_TYPE_VAR_STRING:
        	// there is a bug in mysql jdbc driver where binary data is sent using type FIELD_TYPE_VAR_STRING. 
        	// for now we temporarily don't support binary in prepared statements. we will deal with this later
        case Fields.FIELD_TYPE_STRING:
        case Fields.FIELD_TYPE_VARCHAR:
            bv.value = mm.readStringWithLength();
            if (bv.value == null) {
                bv.isNull = true;
            }
            break;
        case Fields.FIELD_TYPE_DECIMAL:
        case Fields.FIELD_TYPE_NEW_DECIMAL:
            bv.value = mm.readBigDecimal();
            if (bv.value == null) {
                bv.isNull = true;
            }
            break;
        default:
            throw new IllegalArgumentException("bindValue error,unsupported type:" + bv.type);
        }
        bv.isSet = true;
    }

    public static long read(Heap heap, ByteBuf buf, int type) {
    	long pValue = 0;
        switch (type & 0xff) {
        case Fields.FIELD_TYPE_TINY:
        	pValue = Int4.allocSet(heap, buf.readByte());
            break;
        case Fields.FIELD_TYPE_SHORT:
        	pValue = Int4.allocSet(heap, (short)BufferUtils.readInt(buf));
            break;
        case Fields.FIELD_TYPE_LONG:
        	pValue = Int4.allocSet(heap, BufferUtils.readLong(buf));
            break;
        case Fields.FIELD_TYPE_LONGLONG:
        	pValue = Int8.allocSet(heap, BufferUtils.readLongLong(buf));
            break;
        case Fields.FIELD_TYPE_FLOAT:
        	pValue = Float4.allocSet(heap, BufferUtils.readFloat(buf));
            break;
        case Fields.FIELD_TYPE_DOUBLE:
        	pValue = Float8.allocSet(heap, BufferUtils.readDouble(buf));
            break;
        case Fields.FIELD_TYPE_TIME:
        case Fields.FIELD_TYPE_TIME2:
        	pValue = FishTime.allocSet(heap, BufferUtils.readTime(buf));
            break;
        case Fields.FIELD_TYPE_DATE:
        	pValue = FishDate.allocSet(heap, BufferUtils.readDate(buf));
            break;
        case Fields.FIELD_TYPE_DATETIME:
        case Fields.FIELD_TYPE_TIMESTAMP:
        case Fields.FIELD_TYPE_DATETIME2:
        case Fields.FIELD_TYPE_TIMESTAMP2:
        	pValue = FishTimestamp.allocSet(heap, BufferUtils.readTimestamp(buf));
            break;
        case Fields.FIELD_TYPE_VAR_STRING:
        case Fields.FIELD_TYPE_STRING:
        case Fields.FIELD_TYPE_VARCHAR:
        	int len = (int)BufferUtils.readLength(buf);
        	long pData = buf.memoryAddress() + buf.readerIndex();
        	pValue = FishUtf8.allocSet(heap, pData, len);
        	buf.readerIndex(buf.readerIndex() + len);
            break;
        case Fields.FIELD_TYPE_DECIMAL:
        case Fields.FIELD_TYPE_NEW_DECIMAL:
        	pValue = FishNumber.allocSet(heap, BufferUtils.readBigDecimal(buf));
            break;
        default:
            throw new IllegalArgumentException("bindValue error,unsupported type:" + type);
        }
        return pValue;
    }
    
    public static long read(Heap heap, ByteBuffer buf, byte type) {
        long pValue = 0;
        switch (type) {
        case Fields.FIELD_TYPE_TINY:
            pValue = Int4.allocSet(heap, buf.get());
            break;
        case Fields.FIELD_TYPE_SHORT:
            pValue = Int4.allocSet(heap, (short)BufferUtils.readInt(buf));
            break;
        case Fields.FIELD_TYPE_LONG:
            pValue = Int4.allocSet(heap, BufferUtils.readLong(buf));
            break;
        case Fields.FIELD_TYPE_LONGLONG:
            pValue = Int8.allocSet(heap, BufferUtils.readLongLong(buf));
            break;
        case Fields.FIELD_TYPE_FLOAT:
            pValue = Float4.allocSet(heap, BufferUtils.readFloat(buf));
            break;
        case Fields.FIELD_TYPE_DOUBLE:
            pValue = Float8.allocSet(heap, BufferUtils.readDouble(buf));
            break;
        case Fields.FIELD_TYPE_TIME:
        case Fields.FIELD_TYPE_TIME2:
            pValue = FishTime.allocSet(heap, BufferUtils.readTime(buf));
            break;
        case Fields.FIELD_TYPE_DATE:
            pValue = FishDate.allocSet(heap, BufferUtils.readDate(buf));
            break;
        case Fields.FIELD_TYPE_DATETIME:
        case Fields.FIELD_TYPE_TIMESTAMP:
        case Fields.FIELD_TYPE_DATETIME2:
        case Fields.FIELD_TYPE_TIMESTAMP2:
            pValue = FishTimestamp.allocSet(heap, BufferUtils.readTimestamp(buf));
            break;
        case Fields.FIELD_TYPE_VAR_STRING:
        case Fields.FIELD_TYPE_STRING:
        case Fields.FIELD_TYPE_VARCHAR:
            int len = (int)BufferUtils.readLength(buf);
            long pData = UberUtil.getAddress(buf) + buf.position();
            pValue = FishUtf8.allocSet(heap, pData, len);
            buf.position(buf.position() + len);
            break;
        case Fields.FIELD_TYPE_DECIMAL:
        case Fields.FIELD_TYPE_NEW_DECIMAL:
            pValue = FishNumber.allocSet(heap, BufferUtils.readBigDecimal(buf));
            break;
        default:
            throw new IllegalArgumentException("bindValue error,unsupported type:" + type);
        }
        return pValue;
    }
    
    public static final void read(ByteBuf buf, BindValue bv) {
        switch (bv.type & 0xff) {
        case Fields.FIELD_TYPE_TINY:
            bv.byteBinding = buf.readByte();
            break;
        case Fields.FIELD_TYPE_SHORT:
            bv.shortBinding = (short)BufferUtils.readInt(buf);
            break;
        case Fields.FIELD_TYPE_LONG:
            bv.intBinding = BufferUtils.readLong(buf);
            break;
        case Fields.FIELD_TYPE_LONGLONG:
            bv.longBinding = BufferUtils.readLongLong(buf);
            break;
        case Fields.FIELD_TYPE_FLOAT:
            bv.floatBinding = BufferUtils.readFloat(buf);
            break;
        case Fields.FIELD_TYPE_DOUBLE:
            bv.doubleBinding = BufferUtils.readDouble(buf);
            break;
        case Fields.FIELD_TYPE_TIME:
        case Fields.FIELD_TYPE_TIME2:
            bv.value = BufferUtils.readTime(buf);
            break;
        case Fields.FIELD_TYPE_DATE:
            bv.value = BufferUtils.readDate(buf);
            break;
        case Fields.FIELD_TYPE_DATETIME:
        case Fields.FIELD_TYPE_TIMESTAMP:
        case Fields.FIELD_TYPE_DATETIME2:
        case Fields.FIELD_TYPE_TIMESTAMP2:
            bv.value = BufferUtils.readTimestamp(buf);
            break;
        case Fields.FIELD_TYPE_VAR_STRING:
            //bv.value = BufferUtils.readBytesWithLength(buf);
            //break;
        case Fields.FIELD_TYPE_STRING:
        case Fields.FIELD_TYPE_VARCHAR:
            bv.value = BufferUtils.readStringWithLength(buf);
            if (bv.value == null) {
                bv.isNull = true;
            }
            break;
        case Fields.FIELD_TYPE_DECIMAL:
        case Fields.FIELD_TYPE_NEW_DECIMAL:
            bv.value = BufferUtils.readBigDecimal(buf);
            if (bv.value == null) {
                bv.isNull = true;
            }
            break;
        default:
            throw new IllegalArgumentException("bindValue error,unsupported type:" + bv.type);
        }
        bv.isSet = true;
    }
}