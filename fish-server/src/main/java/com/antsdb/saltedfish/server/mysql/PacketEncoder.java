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

import io.netty.buffer.ByteBuf;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;

import org.apache.commons.codec.Charsets;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.slf4j.Logger;

import static com.antsdb.saltedfish.server.mysql.MysqlConstant.*;

import com.antsdb.saltedfish.cpp.FishBool;
import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Float4;
import com.antsdb.saltedfish.cpp.Float8;
import com.antsdb.saltedfish.cpp.Int4;
import com.antsdb.saltedfish.cpp.Int8;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.server.mysql.packet.MySQLPacket;
import com.antsdb.saltedfish.server.mysql.util.BufferUtils;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.Record;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.util.Callback;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * This class encode data into packet and 
 * @author roger
 */
public final class PacketEncoder {
    static Logger _log = UberUtil.getThisLogger();
    
    // for a simple OK packet, no need to call back, just out put hard coded bytes
    public static final byte[] OK_PACKET = new byte[] { 7, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0 };
    // for a auth OK packet, no need to call back, just out put hard coded bytes
    public static final byte[] AUTH_OK_PACKET = new byte[] { 7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0 };
    static final FastDateFormat TIMESTAMP19_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
    static final FastDateFormat TIMESTAMP29_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS000000");
    
	private Charset cs;
	private int csidx;

    /**
     * Add header to finish the full packet
     * @param out
     * @param packetSeq
     * @param writeBodyFunc        write packet body function
     */
    public static void writePacket(ByteBuf out, byte packetSeq, Callback writeBodyFunc) {
        int start = out.writerIndex();
        out.writeZero(4);
        writeBodyFunc.callback();
        int end = out.writerIndex();
        out.writeByte(0);
        out.writerIndex(start);
        int length = end - start - MySQLPacket.packetHeaderSize;
        BufferUtils.writeLongInt(out, length);
        out.writeByte(packetSeq);
        out.writerIndex(end);
        if (_log.isTraceEnabled()) {
            int readerIndex = out.readerIndex();
            out.readerIndex(start);
            byte[] bytes = new byte[end-start];
            out.readBytes(bytes);
            out.readerIndex(readerIndex);
            String dump = '\n' + UberUtil.hexDump(bytes);
            _log.trace(dump);
        }
    }

    /**
     * Writer com_stmt_prepare_response packet body
     * <pre>
     * From server to client, in response to prepared statement initialization packet. 
     * It is made up of: 
     *   1.a PREPARE_OK packet
     *   2.if "number of parameters" > 0 
     *       (field packets) as in a Result Set Header Packet 
     *       (EOF packet)
     *   3.if "number of columns" > 0 
     *       (field packets) as in a Result Set Header Packet 
     *       (EOF packet)
     *   
     * -----------------------------------------------------------------------------------------
     * 
     *  Bytes              Name
     *  -----              ----
     *  1                  0 - marker for OK packet
     *  4                  statement_handler_id
     *  2                  number of columns in result set
     *  2                  number of parameters in query
     *  1                  filler (always 0)
     *  2                  warning count
     *  
     *  @see http://dev.mysql.com/doc/internals/en/prepared-statement-initialization-packet.html
     * </pre>
     * @param buffer
     * @param statementId
     * @param columnsNumber
     * @param parametersNumber
     */
    public void writePreparedOKBody(ByteBuf buffer, long statementId, int columnsNumber, int parametersNumber) {
        // flag = 0
        buffer.writeByte(0);
        BufferUtils.writeUB4(buffer, statementId);
        BufferUtils.writeUB2(buffer, columnsNumber);
        BufferUtils.writeUB2(buffer, parametersNumber);
        // filler = 0
        buffer.writeByte(0);
        // warningCount = 0;
        BufferUtils.writeUB2(buffer, 0);
    }

    /**
     * 
     * From server to client after command, if no error and result set -- that is,
     * if the command was a query which returned a result set. The Result Set Header
     * Packet is the first of several, possibly many, packets that the server sends
     * for result sets. The order of packets for a result set is:
     * 
     * <pre>
     * (Result Set Header Packet)   the number of columns
     * (Field Packets)              column descriptors
     * (EOF Packet)                 marker: end of Field Packets
     * (Row Data Packets)           row contents
     * (EOF Packet)                 marker: end of Data Packets
     * 
     * Bytes                        Name
     * -----                        ----
     * 1-9   (Length-Coded-Binary)  field_count
     * 1-9   (Length-Coded-Binary)  extra
     * 
     * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Result_Set_Header_Packet
     * </pre>
     * 
     * @param buffer
     * @param meta
     */
    public void writeColumnDefBody(ByteBuf buffer, FieldMeta meta) {
        //catalog
        BufferUtils.writeLenString(buffer, "def", this.cs);
        //db, schema
        if (meta.getSourceTable() != null) {
        	BufferUtils.writeLenString(buffer, meta.getSourceTable().getNamespace(), this.cs);
        }
        else {
        	BufferUtils.writeLenString(buffer, "", this.cs);
        }
        // table
        BufferUtils.writeLenString(buffer, meta.getTableAlias(), this.cs);
        // orgTable
        if (meta.getSourceTable() != null) {
        	BufferUtils.writeLenString(buffer, meta.getSourceTable().getTableName(), this.cs);
        }
        else {
        	BufferUtils.writeLenString(buffer, "", this.cs);
        }
        // col name
        BufferUtils.writeLenString(buffer, meta.getName(), this.cs);
        // col original name
        BufferUtils.writeLenString(buffer, meta.getSourceName(), this.cs);
        // next length 
        buffer.writeByte((byte) 0x0C);
        if (meta.getType() == null) {
            BufferUtils.writeInt(buffer, 0x3f);
            BufferUtils.writeUB4(buffer, 0);
            buffer.writeByte((byte) (FIELD_TYPE_NULL & 0xff));
            buffer.writeByte(0x00);
            buffer.writeByte(0x00);
            buffer.writeByte(0);
        }
        else if (meta.getType().getJavaType() == Boolean.class) {
            BufferUtils.writeInt(buffer, 0x3f);
            BufferUtils.writeUB4(buffer, meta.getType().getLength());
            buffer.writeByte((byte) (FIELD_TYPE_TINY & 0xff));
            buffer.writeByte(0x00);
            buffer.writeByte(0x00);
            buffer.writeByte(0);
        }
        else if (meta.getType().getSqlType() == Types.TINYINT) {
            BufferUtils.writeInt(buffer, 0x3f);
            BufferUtils.writeUB4(buffer, meta.getType().getLength());
            buffer.writeByte((byte) (FIELD_TYPE_TINY & 0xff));
            buffer.writeByte(0x00);
            buffer.writeByte(0x00);
            buffer.writeByte((byte)meta.getType().getScale());
        }
        else if (meta.getType().getJavaType() == String.class) {
            // char set utf8_general_ci  : 0x21
            BufferUtils.writeInt(buffer, this.csidx);
            // length
            BufferUtils.writeUB4(buffer, meta.getType().getLength() * 3);
            // type code
            buffer.writeByte((byte) (FIELD_TYPE_VAR_STRING & 0xff));
            buffer.writeByte(0x00);
            buffer.writeByte(0x00);
            buffer.writeByte(0);
        }
        else if (meta.getType().getJavaType() == Integer.class) {
            BufferUtils.writeInt(buffer, 0x3f);
            BufferUtils.writeUB4(buffer, 21);
            buffer.writeByte((byte) (FIELD_TYPE_LONG & 0xff));
            buffer.writeByte(0x00);
            buffer.writeByte(0x00);
            buffer.writeByte(0);
        }
        else if (meta.getType().getJavaType() == Long.class) {
            BufferUtils.writeInt(buffer, 0x3f);
            BufferUtils.writeUB4(buffer, 21);
            buffer.writeByte((byte) (FIELD_TYPE_LONGLONG & 0xff));
            buffer.writeByte(0x00); // signed.
            buffer.writeByte(0x00);
            buffer.writeByte(0);
        }
        else if (meta.getType().getJavaType() == BigInteger.class) {
            BufferUtils.writeInt(buffer, 0x3f);
            BufferUtils.writeUB4(buffer, meta.getType().getLength());
            buffer.writeByte((byte) (FIELD_TYPE_LONGLONG & 0xff));
            buffer.writeByte(0x00);
            buffer.writeByte(0x00);
            buffer.writeByte(0);
        }
        else if (meta.getType().getJavaType() == BigDecimal.class) {
            BufferUtils.writeInt(buffer, 0x3f);
            BufferUtils.writeUB4(buffer, meta.getType().getLength());
            buffer.writeByte((byte) (FIELD_TYPE_DECIMAL & 0xff));
            buffer.writeByte(0x00);
            buffer.writeByte(0x00);
            buffer.writeByte((byte)meta.getType().getScale());
        }
        else if (meta.getType().getJavaType() == Float.class) {
            BufferUtils.writeInt(buffer, 0x3f);
            BufferUtils.writeUB4(buffer, meta.getType().getLength());
            buffer.writeByte((byte) (FIELD_TYPE_FLOAT & 0xff));
            buffer.writeByte(0x00);
            buffer.writeByte(0x00);
            buffer.writeByte((byte)meta.getType().getScale());
        }
        else if (meta.getType().getJavaType() == Double.class) {
            BufferUtils.writeInt(buffer, 0x3f);
            BufferUtils.writeUB4(buffer, meta.getType().getLength());
            buffer.writeByte((byte) (FIELD_TYPE_DOUBLE & 0xff));
            buffer.writeByte(0x00);
            buffer.writeByte(0x00);
            buffer.writeByte((byte)meta.getType().getScale());
        }
        else if (meta.getType().getJavaType() == Timestamp.class) {
            BufferUtils.writeInt(buffer, 0x3f);
            BufferUtils.writeUB4(buffer, meta.getType().getLength());
            buffer.writeByte((byte) (FIELD_TYPE_TIMESTAMP & 0xff));
            buffer.writeByte(0x00);
            buffer.writeByte(0x00);
            buffer.writeByte((byte)meta.getType().getScale());
        }
        else if (meta.getType().getJavaType() == Date.class) {
            BufferUtils.writeInt(buffer, 0x3f);
            BufferUtils.writeUB4(buffer, meta.getType().getLength());
            buffer.writeByte((byte) (FIELD_TYPE_DATE & 0xff));
            buffer.writeByte(0x00);
            buffer.writeByte(0x00);
            buffer.writeByte((byte)meta.getType().getScale());
        }
        else if (meta.getType().getJavaType() == Time.class) {
            BufferUtils.writeInt(buffer, 0x3f);
            BufferUtils.writeUB4(buffer, meta.getType().getLength());
            buffer.writeByte((byte) (FIELD_TYPE_TIME & 0xff));
            buffer.writeByte(0x00);
            buffer.writeByte(0x00);
            buffer.writeByte((byte)meta.getType().getScale());
        }
        // BLOB return byte[] as its java type
        else if (meta.getType().getJavaType() == byte[].class) {
            BufferUtils.writeInt(buffer, 0x3f);
            BufferUtils.writeUB4(buffer, 2147483647);
            buffer.writeByte((byte) (FIELD_TYPE_BLOB & 0xff));
            // flag for Blob is x90 x00
            buffer.writeByte(0x90);
            buffer.writeByte(0x00);
            buffer.writeByte((byte)meta.getType().getScale());
        }
        else {
            throw new NotImplementedException("Unsupported data type:" + meta.getType().getJavaType());
        }
        /**
         * need add meta info to specify flag
        if (meta.isNullable() == 1) {
            flags |= 0001;
        }

        if (meta.isSigned()) {
            flags |= 0020;
        }

        if (meta.isAutoIncrement()) {
            flags |= 0200;
        }
        */
        // filler
        buffer.writeZero(2);
    }

    /**
     * From server to client after command, if no error and result set -- that is,
     * if the command was a query which returned a result set. The Result Set Header
     * Packet is the first of several, possibly many, packets that the server sends
     * for result sets. The order of packets for a result set is:
     * 
     * <pre>
     * (Result Set Header Packet)   the number of columns
     * (Field Packets)              column descriptors
     * (EOF Packet)                 marker: end of Field Packets
     * (Row Data Packets)           row contents
     * (EOF Packet)                 marker: end of Data Packets
     * 
     * Bytes                        Name
     * -----                        ----
     * 1-9   (Length-Coded-Binary)  field_count
     * 1-9   (Length-Coded-Binary)  extra
     * 
     * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Result_Set_Header_Packet
     * </pre>
     * 
     * @param buffer
     * @param fieldCount
     */
    public void writeResultSetHeaderBody(ByteBuf buffer, int fieldCount) {
        // field count
        BufferUtils.writeLength(buffer, fieldCount);

    }

    /**
     * 
     * From server to client. One packet for each row in the result set.
     * 
     * <pre>
     * Bytes                   Name
     * -----                   ----
     * n (Length Coded String) (column value)
     * ...
     * 
     * (column value):         The data in the column, as a character string.
     *                         If a column is defined as non-character, the
     *                         server converts the value into a character
     *                         before sending it. Since the value is a Length
     *                         Coded String, a NULL can be represented with a
     *                         single byte containing 251(see the description
     *                         of Length Coded Strings in section "Elements" above).
     * 
     * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Row_Data_Packet
     * </pre>
     * 
     * @param buffer
     * @param fieldValues
     */
    public void writeRowBinaryBody(ByteBuf buffer, long pRecord, CursorMeta meta, int nColumns) {
        if ((pRecord != 0) && (nColumns >0))
        {
            // start of package
            buffer.writeByte(0);
            
            int nullByteCnt = (nColumns+7+2)/8;

            byte[] nullBitmap = new byte[nullByteCnt];
            int nullPos = buffer.writerIndex();
            
            buffer.writeBytes(nullBitmap);

            for (int i=0; i<nColumns; i++)
            {
            	long pValue = Record.getValueAddress(pRecord, i);
            	if (pValue != 0) {
                	writeValue(buffer, meta.getColumn(i), pValue);
            	}
            	else {
                	nullBitmap[(i+2)/8] |= 1 << (i+2)%8;
            	}
            }
    
            int endPos = buffer.writerIndex();
            
            buffer.writerIndex(nullPos);
            buffer.writeBytes(nullBitmap);

            buffer.writerIndex(endPos);
        }
    }

    private void writeValue(ByteBuf buffer, FieldMeta meta, long pValue) {
    	if (writeValueFast(buffer, meta, pValue)) {
    		return;
    	}
    	writeValueSlow(buffer, meta, pValue);
	}

	private boolean writeValueFast(ByteBuf buffer, FieldMeta meta, long pValue) {
		DataType type = meta.getType();
		byte format = Value.getFormat(null, pValue);
        if (type.getSqlType() == Types.TINYINT) {
        	if (format == Value.FORMAT_INT4) {
        		buffer.writeByte(Int4.get(pValue));
        		return true;
        	}
        	else if (format == Value.FORMAT_INT8) {
        		buffer.writeByte((int)Int8.get(null, pValue));
        		return true;
        	}
        }
        else if (type.getJavaType()==Boolean.class) {
        	boolean b = FishBool.get(null, pValue);
    		buffer.writeByte(b ? 1 : 0);
    		return true;
        }
        else if (type.getJavaType()==Integer.class) {
        	if (format == Value.FORMAT_INT4) {
        		BufferUtils.writeUB4(buffer, Int4.get(pValue));
        		return true;
        	}
        	else if (format == Value.FORMAT_INT8) {
        		BufferUtils.writeUB4(buffer, (int)Int8.get(null, pValue));
        		return true;
        	}
        }
        else if (type.getJavaType()==Long.class) {
        	if (format == Value.FORMAT_INT4) {
        		BufferUtils.writeLongLong(buffer, Int4.get(pValue));
        		return true;
        	}
        	else if (format == Value.FORMAT_INT8) {
        		BufferUtils.writeLongLong(buffer, Int8.get(null, pValue));
        		return true;
        	}
        }
        else if (type.getJavaType()==Float.class) {
        	if (format == Value.FORMAT_FLOAT4) {
                BufferUtils.writeUB4(buffer, Float.floatToIntBits(Float4.get(null, pValue)));
        		return true;
        	}
        }
        else if (type.getJavaType()==Double.class) {
        	if (format == Value.FORMAT_FLOAT4) {
                BufferUtils.writeLongLong(buffer,Double.doubleToLongBits(Float8.get(null, pValue)));
        		return true;
        	}
        }
		return false;
	}
	
	private void writeValueSlow(ByteBuf buffer, FieldMeta meta, long pValue) {
		Object value = FishObject.get(null, pValue);
		DataType type = meta.getType();
        if (type.getSqlType() == Types.TINYINT) {
            buffer.writeByte((Integer)value);
        }
        else if (type.getJavaType()==Integer.class) {
            BufferUtils.writeUB4(buffer,(Integer)value);
        }
        else if (type.getJavaType()==Long.class) {
            BufferUtils.writeLongLong(buffer,(Long)value);
        }
        else if (type.getJavaType()==Float.class) {
            BufferUtils.writeUB4(buffer, Float.floatToIntBits((Float)value));
        }
        else if (type.getJavaType()==Double.class) {
            BufferUtils.writeLongLong(buffer,Double.doubleToLongBits((Double)value));
        }
        else if (type.getJavaType()==Timestamp.class) {
            BufferUtils.writeTimestamp(buffer, (Timestamp)value);
        }
        else if (type.getJavaType()==Date.class) {
            BufferUtils.writeDate(buffer, (Date)value);
        }
        else if (type.getJavaType()==byte[].class) {
            BufferUtils.writeWithLength(buffer, (byte[])value);
        }
        else {
            BufferUtils.writeLenString(buffer,value.toString(), this.cs);
        }
	}

	/**
     * 
     * From server to client. One packet for each row in the result set.
     * 
     * <pre>
     * Bytes                   Name
     * -----                   ----
     * n (Length Coded String) (column value)
     * ...
     * 
     * (column value):         The data in the column, as a character string.
     *                         If a column is defined as non-character, the
     *                         server converts the value into a character
     *                         before sending it. Since the value is a Length
     *                         Coded String, a NULL can be represented with a
     *                         single byte containing 251(see the description
     *                         of Length Coded Strings in section "Elements" above).
     * 
     * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Row_Data_Packet
     * </pre>
     * 
     * @param buffer
     * @param nColumns 
     * @param rowRec
     */
    public void writeRowTextBody(ByteBuf buffer, long pRecord, int nColumns) {
        for (int i = 0; i < nColumns; i++) {
            Object fv = Record.getValue(pRecord, i);
            if (fv instanceof Boolean) {
            	// mysql has no boolean it is actually tinyint
            	fv = ((Boolean)fv) ? 1 : 0;
            }
            if (fv == null) {
                // null mark is 251
                buffer.writeByte((byte) 251);
            } 
            else if (fv instanceof Duration) {
            	Duration t = (Duration)fv;
            	String text = DurationFormatUtils.formatDuration(t.toMillis(), "HH:mm:ss");
            	BufferUtils.writeLenString(buffer, text, this.cs);
            }
            else if (fv instanceof Timestamp) {
                // @see ResultSetRow#getDateFast, mysql jdbc driver only take precision 19,21,29 if callers wants
                // to get a Date from a datetime column
            	Timestamp ts = (Timestamp)fv;
            	if (ts.getTime() == Long.MIN_VALUE) {
                	// mysql '0000-00-00 00:00:00' is treated as null in jdbc
                    buffer.writeByte((byte) 251);
            	}
            	else {
                	String text;
                	if (ts.getNanos() == 0) {
                		text = TIMESTAMP19_FORMAT.format(ts);
                	}
                	else {
                		text = TIMESTAMP29_FORMAT.format(ts);
                	}
                    BufferUtils.writeLenString(buffer, text, this.cs);
            	}
            }
            else if (fv instanceof byte[]){
                BufferUtils.writeWithLength(buffer, (byte[])fv);
            }
            else if ((fv instanceof Date) && (((Date)fv).getTime() == Long.MIN_VALUE)) {
            	// mysql '0000-00-00' is treated as null in jdbc
                buffer.writeByte((byte) 251);
            }
            else 
            {
                String val = fv.toString();
                if (val.length() == 0) {
                    // empty mark is 0
                    buffer.writeByte((byte) 0);
                } 
                else {
                    BufferUtils.writeLenString(buffer, val, this.cs);
                }
            }
        }
    }

    
    /**
     * 
     * From server to client during initial handshake.
     * 
     * <pre>
     * Bytes                        Name
     * -----                        ----
     * 1                            protocol_version
     * n (Null-Terminated String)   server_version
     * 4                            thread_id
     * 8                            scramble_buff
     * 1                            (filler) always 0x00
     * 2                            server_capabilities
     * 1                            server_language
     * 2                            server_status
     * 13                           (filler) always 0x00 ...
     * 13                           rest of scramble_buff (4.1)
     * 
     * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Handshake_Initialization_Packet
     * </pre>
     * 
     * @param buffer
     * @param serverVersion
     * @param protocolVersion
     * @param threadId
     * @param capability
     * @param charSet
     * @param status
     */
    public static void writeHandshakeBody(ByteBuf buffer, 
    		                              String serverVersion, 
    		                              byte protocolVersion, 
    		                              long threadId, 
    		                              int capability, 
    		                              byte charSet, 
    		                              int status) {
        buffer.writeByte(protocolVersion);
        BufferUtils.writeString(buffer, serverVersion);
        BufferUtils.writeUB4(buffer, threadId);
        // seed
        byte[] seed  = new byte[] {0x50, 0x3a, 0x6e, 0x3d, 0x25, 0x40, 0x51, 0x56};
        buffer.writeBytes(seed);
        buffer.writeByte(0);
        // lower 16 bits of sever capacity
        BufferUtils.writeInt(buffer, capability);
        // serverCharsetIndex
        buffer.writeByte(charSet);
        // server status 
        BufferUtils.writeInt(buffer, status);
        // upper 16 bits of server capacity
        BufferUtils.writeInt(buffer, capability >>> 16);
        // plugin data length
        if ((capability & MysqlServerHandler.CLIENT_PLUGIN_AUTH) != 0) {
        	buffer.writeByte(0x15);
        }
        else {
        	buffer.writeByte(0);
        }
        // fill the rest 10 bytes with 0
        buffer.writeZero(10);
        if ((capability & MysqlServerHandler.CLIENT_PLUGIN_AUTH) != 0) {
        	// no idea what this means, copied from trace
        	buffer.writeBytes(new byte[] {0x73, 0x68, 0x2f, 0x50, 0x27, 0x6f, 0x7a, 0x38, 0x46, 0x38, 0x26, 0x51, 0x00});
        	BufferUtils.writeString(buffer, MysqlServerHandler.AUTH_MYSQL_NATIVE);
        }
    }

    /**
     * 
     * From server to client in response to command, if no error and no result set.
     * 
     * <pre>
     * Bytes                       Name
     * -----                       ----
     * 1                           field_count, always = 0
     * 1-9 (Length Coded Binary)   affected_rows
     * 1-9 (Length Coded Binary)   insert_id
     * 2                           server_status
     * 2                           warning_count
     * n   (until end of packet)   message fix:(Length Coded String)
     * 
     * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#OK_Packet
     * </pre>
     * 
     * @param buffer
     */
    public void writeOKBody(ByteBuf buffer, long affectedRows, long insertId, String message, Session session) {
        // filed count
        buffer.writeByte(0x00);
        // affected rows
        BufferUtils.writeLength(buffer, affectedRows);
        // inserId
        BufferUtils.writeLength(buffer, insertId);
        // server status
        int status = 0;
        if (session.isAutoCommit()) {
        	status |= SERVER_STATUS_AUTOCOMMIT;
        }
        Transaction trx = session.getTransaction();
        if (trx != null) {
        	if (trx.getTrxId() < 0) {
        		status |= SERVER_STATUS_IN_TRANS;
        	}
        }
        BufferUtils.writeUB2(buffer, status);
        // warning count = 0
        BufferUtils.writeUB2(buffer, 0);
        // message
        if (message != null) {
            BufferUtils.writeLenString(buffer, message, Charsets.UTF_8);
        }
    }

    /**
     * 
     * From server to client in response to command, if error.
     * 
     * <pre>
     * Bytes                       Name
     * -----                       ----
     * 1                           field_count, always = 0xff
     * 2                           errno
     * 1                           (sqlstate marker), always '#'
     * 5                           sqlstate (5 characters)
     * n                           message
     * 
     * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Error_Packet
     * </pre>
     * 
     * @param buffer
     * @param errno
     * @param message
     */
    public void writeErrorBody(ByteBuf buffer, int errno, ByteBuffer message) {
        // field count
        buffer.writeByte((byte) 0xff);
        // error number
        BufferUtils.writeUB2(buffer, errno);
        // sql state mark
        buffer.writeByte((byte) '#');
        // sql state
        buffer.writeBytes("HY000".getBytes());
        if (message != null) {
            buffer.writeBytes(message);
        }
    }

    /**
     * 
     * From Server To Client, at the end of a series of Field Packets, and at the
     * end of a series of Data Packets.With prepared statements, EOF Packet can also
     * end parameter information, which we'll describe later.
     * 
     * <pre>
     * Bytes                 Name
     * -----                 ----
     * 1                     field_count, always = 0xfe
     * 2                     warning_count
     * 2                     Status Flags
     * 
     * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#EOF_Packet
     * </pre>
     * 
     * 
     * @param out
     * @param session 
     */
    public void writeEOFBody(ByteBuf out, Session session) {
        // field count
        out.writeByte((byte) 0xfe);
        // warning count = 0
        BufferUtils.writeUB2(out, 0);
        // status = 2
        int status = 0x20;
        if (session.isAutoCommit()) {
        	status |= SERVER_STATUS_AUTOCOMMIT;
        }
        Transaction trx = session.getTransaction();
        if (trx != null) {
        	if (trx.getTrxId() < 0) {
        		status |= SERVER_STATUS_IN_TRANS;
        	}
        }
        BufferUtils.writeUB2(out, status);
    }

    /**
     * 
     * Registers a slave at the master. Should be sent before requesting a binlog events with COM_BINLOG_DUMP.
     * 
     * <pre>
     * Bytes                        Name
     * -----                        ----
     *	1              [15] COM_REGISTER_SLAVE
     *	4              server-id
     *	1              slaves hostname length
     *	string[$len]   slaves hostname
     *	1              slaves user len
     *	string[$len]   slaves user
     *	1              slaves password len
     *	string[$len]   slaves password
     *	2              slaves mysql-port
     *	4              replication rank
     *	4              master-id
     * 
     * @see https://dev.mysql.com/doc/internals/en/com-register-slave.html
     * </pre>
     * 
     */
    public void writeRegisterSlave(ByteBuf buffer, int serverId) {
        // code for COM_REGISTER_SLAVE is 0x15 
    	buffer.writeByte(0x15);
        BufferUtils.writeUB4(buffer, serverId);
        // usually empty
        BufferUtils.writeLenString(buffer, "", Charsets.UTF_8);
        // usually empty
        BufferUtils.writeLenString(buffer, "", Charsets.UTF_8);
        // usually empty
        BufferUtils.writeLenString(buffer, "", Charsets.UTF_8);
        // usually empty
        BufferUtils.writeUB2(buffer, 0);
        // replication rank to be ignored
        buffer.writeBytes(new byte[4]);
        // master id, usually 0
        BufferUtils.writeUB4(buffer, 0);
    }


    /**
     * 
     * Requests a binlog network stream from the master starting a given position.
     * 
     * <pre>
     * Bytes                        Name
     * -----                        ----
     *	1              [12] COM_BINLOG_DUMP
     *	4              binlog-pos
     *	2              flags
     *	4              server-id
     *	string[EOF]    binlog-filename
     * 
     * @see https://dev.mysql.com/doc/internals/en/com-binlog-dump.html
     * </pre>
     * 
     */
    public void writeBinlogDump(ByteBuf buffer, 
    		                              long binlogPos, 
    		                              int serverId,
    		                              String binlogName
    		                              ) {
        // code for COM_BINLOG_DUMP is 0x12 
    	buffer.writeByte(0x12);
        BufferUtils.writeUB4(buffer, binlogPos);
        // flag, 0 means don't send EOF if no more bing log
        BufferUtils.writeUB2(buffer, 0);
        // server id of this slave
        BufferUtils.writeUB4(buffer, serverId);
        // binlog file name
        BufferUtils.writeString(buffer, binlogName);
    }

    /**
     * 
     * Handshake responce for replication master.
     * 
     * <pre>
     * Bytes                        Name
     * -----                        ----
     *	2              capability flags, CLIENT_PROTOCOL_41 never set
     *	3              max-packet size
     *	string[NUL]    username
     *	string[NUL]    auth-response
     *	string[NUL]    database
     * 
     * @see https://dev.mysql.com/doc/internals/en/com-binlog-dump.html
     * </pre>
     * 
     */
    public void writeHandshakeResponse(ByteBuf buf, 
    										int capability, 
    										String user,
    										String password
    		                              ) {
    	// capability flags
    	BufferUtils.writeInt(buf, 42117);
    	// max-packet size, 0?
    	BufferUtils.writeLongInt(buf, 0);
    	BufferUtils.writeString(buf, user);
    	BufferUtils.writeString(buf, password);
    	// default dbname
    	BufferUtils.writeString(buf, "");
    }
    
    public void setCodec(Charset cs, int csidx) {
    	this.cs = cs;
    	this.csidx = csidx;
    }
}