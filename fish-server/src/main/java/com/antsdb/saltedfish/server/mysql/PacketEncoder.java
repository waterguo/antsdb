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
package com.antsdb.saltedfish.server.mysql;

import static com.antsdb.saltedfish.server.mysql.MysqlConstant.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

import org.apache.commons.codec.Charsets;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.FishBool;
import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Float4;
import com.antsdb.saltedfish.cpp.Float8;
import com.antsdb.saltedfish.cpp.Int4;
import com.antsdb.saltedfish.cpp.Int8;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.parquet.OrcaObjectStoreException;
import com.antsdb.saltedfish.server.mysql.packet.MySQLPacket;
import com.antsdb.saltedfish.server.mysql.packet.PacketType;
import com.antsdb.saltedfish.sql.AuthPlugin;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;
import com.antsdb.saltedfish.sql.vdm.Record;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.util.PacketCallback;
import com.antsdb.saltedfish.util.UberFormatter;
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
    public static final byte[] SSL_AUTH_OK_PACKET = new byte[] { 7, 0, 0, 3, 0, 0, 0, 2, 0, 0, 0 };
    static final FastDateFormat TIMESTAMP19_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
    static final FastDateFormat TIMESTAMP29_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS000000");

    static final FastDateFormat TIME_FORMAT = FastDateFormat.getInstance("HH:mm:ss");

    private PacketWriter packet = new PacketWriter();
    int packetSequence = -1;
    /** character set for result set */
    private Supplier<Charset> cs;
    /** character set for metadata */
    private Supplier<Charset> metacs;

    public PacketEncoder(MysqlSession mysession) {
        this(() -> {
            Charset result = null;
            if (mysession.session != null) {
                result = mysession.session.getConfig().getResultEncoder();
            }
            if (result == null) {
                result = mysession.fish.getOrca().getDefaultSession().getConfig().getResultEncoder();
            }
            return result;
        },
        ()->{
            Charset result = null;
            if (mysession.session != null) {
                result = mysession.session.getConfig().getMetadataEncoder(); 
            }
            if (result == null) {
                result = mysession.fish.getOrca().getDefaultSession().getConfig().getMetadataEncoder();
            }
            return result;
        }
        );
    }
    
    public PacketEncoder(Supplier<Charset> cs, Supplier<Charset> metacs) {
        this.cs = cs;
        this.metacs = metacs;
    }

    /**
     * Add header to finish the full packet
     * @param out
     * @param packetSeq
     * @param writeBodyFunc        write packet body function
     */
    public void writePacket(ChannelWriter out, PacketCallback writeBodyFunc) {
        this.packet.clear();
        int start = this.packet.position();
        this.packet.writeLong(0);
        writeBodyFunc.callback(this.packet);
        int end = this.packet.position();
        this.packet.writeByte((byte) 0);
        this.packet.position(start);
        int length = end - start - MySQLPacket.packetHeaderSize;
        this.packet.writeLongInt(length);
        this.packet.writeByte((byte) ++this.packetSequence);
        this.packet.position(end);
        this.packet.flush(out);
        if (_log.isTraceEnabled()) {
            byte[] bytes = new byte[end - start];
            packet.readBytes(start, bytes);
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
    public void writePreparedOKBody(PacketWriter buffer, long statementId, int columnsNumber, int parametersNumber) {
        // flag = 0
        buffer.writeByte((byte) 0);
        buffer.writeUB4(statementId);
        buffer.writeUB2(columnsNumber);
        buffer.writeUB2(parametersNumber);
        // filler = 0
        buffer.writeByte((byte) 0);
        // warningCount = 0;
        buffer.writeUB2(0);
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
     * @see https://mariadb.com/kb/en/library/resultset/#column-definition-packet
     * </pre>
     * 
     * @param buffer
     * @param meta
     */
    public void writeColumnDefBody(PacketWriter buffer, FieldMeta meta) {
        Charset encoder = getMetadataEncoder();
        // catalog
        buffer.writeLenString("def", encoder);
        // db, schema
        if (meta.getSourceTable() != null) {
            buffer.writeLenString(meta.getSourceTable().getNamespace(), encoder);
        }
        else {
            buffer.writeLenString("", encoder);
        }
        // table
        buffer.writeLenString(meta.getTableAlias(), encoder);
        // orgTable
        if (meta.getSourceTable() != null) {
            buffer.writeLenString(meta.getSourceTable().getTableName(), encoder);
        }
        else {
            buffer.writeLenString("", encoder);
        }
        // col alias
        buffer.writeLenString(meta.getName(), encoder);
        // col original name
        buffer.writeLenString(meta.getSourceName(), encoder);
        // length of fixed fields
        buffer.writeByte((byte) 0x0C);
        if (meta.getType() == null) {
            buffer.writeInt(0x3f);
            buffer.writeUB4(0);
            buffer.writeByte((byte) (FIELD_TYPE_NULL & 0xff));
            buffer.writeInt(getFieldDetailFlag(meta));
            buffer.writeByte((byte) 0);
        }
        else if (meta.getType().getJavaType() == Boolean.class) {
            buffer.writeInt(0x3f);
            buffer.writeUB4(meta.getType().getLength());
            buffer.writeByte((byte) (FIELD_TYPE_TINY & 0xff));
            buffer.writeInt(getFieldDetailFlag(meta));
            buffer.writeByte((byte) 0);
        }
        else if (meta.getType().getSqlType() == Types.TINYINT) {
            buffer.writeInt(0x3f);
            buffer.writeUB4(meta.getType().getLength());
            buffer.writeByte((byte) (FIELD_TYPE_TINY & 0xff));
            buffer.writeInt(getFieldDetailFlag(meta));
            buffer.writeByte((byte) meta.getType().getScale());
        }
        else if (meta.getType().getJavaType() == String.class) {
            // char set utf8_general_ci : 0x21
            buffer.writeInt(getCharSetId(getEncoder()));
            // buffer.writeInt(8);
            // length
            buffer.writeUB4(meta.getType().getLength() * 3);
            // type code
            buffer.writeByte((byte) (FIELD_TYPE_VAR_STRING & 0xff));
            buffer.writeInt(getFieldDetailFlag(meta));
            buffer.writeByte((byte) 0);
        }
        else if (meta.getType().getJavaType() == Integer.class) {
            buffer.writeInt(0x3f);
            buffer.writeUB4(21);
            buffer.writeByte((byte) (FIELD_TYPE_LONG & 0xff));
            buffer.writeInt(getFieldDetailFlag(meta));
            buffer.writeByte((byte) 0);
        }
        else if (meta.getType().getJavaType() == Long.class) {
            buffer.writeInt(0x3f);
            buffer.writeUB4(21);
            buffer.writeByte((byte) (FIELD_TYPE_LONGLONG & 0xff));
            buffer.writeInt(getFieldDetailFlag(meta));
            buffer.writeByte((byte) 0);
        }
        else if (meta.getType().getJavaType() == BigInteger.class) {
            buffer.writeInt(0x3f);
            buffer.writeUB4(meta.getType().getLength());
            buffer.writeByte((byte) (FIELD_TYPE_LONGLONG & 0xff));
            buffer.writeInt(getFieldDetailFlag(meta));
            buffer.writeByte((byte) 0);
        }
        else if (meta.getType().getJavaType() == BigDecimal.class) {
            buffer.writeInt(0x3f);
            buffer.writeUB4(meta.getType().getLength());
            buffer.writeByte((byte) (FIELD_TYPE_DECIMAL & 0xff));
            buffer.writeInt(getFieldDetailFlag(meta));
            buffer.writeByte((byte) meta.getType().getScale());
        }
        else if (meta.getType().getJavaType() == Float.class) {
            buffer.writeInt(0x3f);
            buffer.writeUB4(meta.getType().getLength());
            buffer.writeByte((byte) (FIELD_TYPE_FLOAT & 0xff));
            buffer.writeInt(getFieldDetailFlag(meta));
            buffer.writeByte((byte) meta.getType().getScale());
        }
        else if (meta.getType().getJavaType() == Double.class) {
            buffer.writeInt(0x3f);
            buffer.writeUB4(meta.getType().getLength());
            buffer.writeByte((byte) (FIELD_TYPE_DOUBLE & 0xff));
            buffer.writeInt(getFieldDetailFlag(meta));
            buffer.writeByte((byte) meta.getType().getScale());
        }
        else if (meta.getType().getJavaType() == Timestamp.class) {
            buffer.writeInt(0x3f);
            buffer.writeUB4(meta.getType().getLength());
            buffer.writeByte((byte) (FIELD_TYPE_TIMESTAMP & 0xff));
            buffer.writeInt(getFieldDetailFlag(meta));
            buffer.writeByte((byte) meta.getType().getScale());
        }
        else if (meta.getType().getJavaType() == Date.class) {
            buffer.writeInt(0x3f);
            buffer.writeUB4(meta.getType().getLength());
            buffer.writeByte((byte) (FIELD_TYPE_DATE & 0xff));
            buffer.writeInt(getFieldDetailFlag(meta));
            buffer.writeByte((byte) meta.getType().getScale());
        }
        else if (meta.getType().getJavaType() == Time.class) {
            buffer.writeInt(0x3f);
            buffer.writeUB4(meta.getType().getLength());
            buffer.writeByte((byte) (FIELD_TYPE_TIME & 0xff));
            buffer.writeInt(getFieldDetailFlag(meta));
            buffer.writeByte((byte) meta.getType().getScale());
        }
        // BLOB return byte[] as its java type
        else if (meta.getType().getJavaType() == byte[].class) {
            if (meta.getType().getSqlType() == Types.BINARY) {
                // char set binary
                buffer.writeInt(MysqlConstant.MYSQL_COLLATION_INDEX_binary);
                // length
                buffer.writeUB4(meta.getType().getLength() * 3);
                // type
                buffer.writeByte((byte) FIELD_TYPE_STRING);
                // flags
                buffer.writeInt((short) (getFieldDetailFlag(meta) | BINARY_FLAG));
                // decimals
                buffer.writeByte((byte) 0);
            }
            else if (meta.getType().getSqlType() == Types.VARBINARY) {
                // char set binary
                buffer.writeInt(0x3f);
                // length
                buffer.writeUB4(meta.getType().getLength() * 3);
                // type
                buffer.writeByte((byte) FIELD_TYPE_VAR_STRING);
                // flags
                buffer.writeInt((short) (getFieldDetailFlag(meta) | BINARY_FLAG));
                // decimals
                buffer.writeByte((byte) 0);
            }
            else {
                buffer.writeInt(0x3f);
                buffer.writeUB4(2147483647);
                buffer.writeByte((byte) (FIELD_TYPE_BLOB & 0xff));
                // flag for Blob is x90 x00
                buffer.writeInt((short) (getFieldDetailFlag(meta) | BINARY_FLAG | BLOB_FLAG));
                buffer.writeByte((byte) meta.getType().getScale());
            }
        }
        else if (meta.getType().getJavaType() == PrimitiveType.class) {
            if (meta.getType().getSqlType() == Types.BINARY) {
                buffer.writeInt(MysqlConstant.MYSQL_COLLATION_INDEX_binary);
                buffer.writeUB4(meta.getType().getLength() * 3);
                buffer.writeByte((byte) FIELD_TYPE_STRING);
                buffer.writeInt((short) (getFieldDetailFlag(meta) | BINARY_FLAG));
                buffer.writeByte((byte) 0);
            }
            else if (meta.getType().getSqlType() == Types.BIGINT || meta.getType().getSqlType() == Types.INTEGER) {
                buffer.writeInt(0x3f);
                buffer.writeUB4(21);
                buffer.writeByte((byte) (FIELD_TYPE_LONGLONG & 0xff));
                buffer.writeInt(getFieldDetailFlag(meta));
                buffer.writeByte((byte) 0);
            }
            else if (meta.getType().getSqlType() == Types.BOOLEAN) {
                buffer.writeInt(0x3f);
                buffer.writeUB4(meta.getType().getLength());
                buffer.writeByte((byte) (FIELD_TYPE_TINY & 0xff));
                buffer.writeInt(getFieldDetailFlag(meta));
                buffer.writeByte((byte) 0);
            }
            else if (meta.getType().getSqlType() == Types.FLOAT) {
                buffer.writeInt(0x3f);
                buffer.writeUB4(meta.getType().getLength());
                buffer.writeByte((byte) (FIELD_TYPE_FLOAT & 0xff));
                buffer.writeInt(getFieldDetailFlag(meta));
                buffer.writeByte((byte) meta.getType().getScale());
            }
            else if (meta.getType().getSqlType() == Types.DOUBLE) {
                buffer.writeInt(0x3f);
                buffer.writeUB4(meta.getType().getLength());
                buffer.writeByte((byte) (FIELD_TYPE_DOUBLE & 0xff));
                buffer.writeInt(getFieldDetailFlag(meta));
                buffer.writeByte((byte) meta.getType().getScale());
            }
            else if (meta.getType().getSqlType() == Types.DECIMAL) {
                buffer.writeInt(0x3f);
                buffer.writeUB4(meta.getType().getLength());
                buffer.writeByte((byte) (FIELD_TYPE_DECIMAL & 0xff));
                buffer.writeInt(getFieldDetailFlag(meta));
                buffer.writeByte((byte) meta.getType().getScale());
            }
            else if (meta.getType().getSqlType() == Types.VARCHAR) {
                buffer.writeInt(getCharSetId(getEncoder()));
                buffer.writeUB4(meta.getType().getLength() * 3);
                buffer.writeByte((byte) (FIELD_TYPE_VAR_STRING & 0xff));
                buffer.writeInt(getFieldDetailFlag(meta));
                buffer.writeByte((byte) 0);
            }
            else if (meta.getType().getSqlType() == Types.DATE) {
                buffer.writeInt(0x3f);
                buffer.writeUB4(meta.getType().getLength());
                buffer.writeByte((byte) (FIELD_TYPE_DATE & 0xff));
                buffer.writeInt(getFieldDetailFlag(meta));
                buffer.writeByte((byte) meta.getType().getScale());
            }
            else if (meta.getType().getSqlType() == Types.TIME) {
                buffer.writeInt(0x3f);
                buffer.writeUB4(meta.getType().getLength());
                buffer.writeByte((byte) (FIELD_TYPE_TIME & 0xff));
                buffer.writeInt(getFieldDetailFlag(meta));
                buffer.writeByte((byte) meta.getType().getScale());
            }
            else if (meta.getType().getSqlType() == Types.TIMESTAMP) {
                buffer.writeInt(0x3f);
                buffer.writeUB4(meta.getType().getLength());
                buffer.writeByte((byte) (FIELD_TYPE_TIMESTAMP & 0xff));
                buffer.writeInt(getFieldDetailFlag(meta));
                buffer.writeByte((byte) meta.getType().getScale());
            }
            else {// default string
                buffer.writeInt(0x3f);
                buffer.writeUB4(meta.getType().getLength() * 3);
                buffer.writeByte((byte) FIELD_TYPE_VAR_STRING);
                buffer.writeInt((short) (getFieldDetailFlag(meta) | BINARY_FLAG));
                buffer.writeByte((byte) 0);
            }
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
        buffer.writeShort((short) 0);
    }

    private short getFieldDetailFlag(FieldMeta meta) {
        short result = 0;
        if (meta.getType() != null) {
            if (meta.getType().isUnsigned()) {
                result |= UNSIGNED_FLAG;
            }
        }
        if (meta.isKeyColumn()) {
            result |= PRI_KEY_FLAG | NOT_NULL_FLAG;
        }
        return result;
    }

    private int getCharSetId(Charset encoder) {
        int result = MysqlConstant.MYSQL_COLLATION_INDEX_utf8_general_ci;
        if (encoder == Charsets.ISO_8859_1) {
            result = MysqlConstant.MYSQL_COLLATION_INDEX_latin1_swedish_ci;
        }
        return result;
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
    public void writeResultSetHeaderBody(PacketWriter buffer, int fieldCount) {
        // field count
        buffer.writeLength(fieldCount);
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
    public void writeRowBinaryBody(PacketWriter buffer, long pRecord, CursorMeta meta, int nColumns) {
        if ((pRecord != 0) && (nColumns > 0)) {
            // start of package
            buffer.writeByte((byte) 0);

            int nullByteCnt = (nColumns + 7 + 2) / 8;

            byte[] nullBitmap = new byte[nullByteCnt];
            int nullPos = buffer.position();

            buffer.writeBytes(nullBitmap);

            for (int i = 0; i < nColumns; i++) {
                long pValue = Record.getValueAddress(pRecord, i);
                if (pValue != 0) {
                    writeBinaryValue(buffer, meta.getColumn(i), pValue);
                }
                else {
                    nullBitmap[(i + 2) / 8] |= 1 << (i + 2) % 8;
                }
            }

            int endPos = buffer.position();

            buffer.position(nullPos);
            buffer.writeBytes(nullBitmap);

            buffer.position(endPos);
        }
    }

    private void writeBinaryValue(PacketWriter buffer, FieldMeta meta, long pValue) {
        if (writeBinaryValueFast(buffer, meta, pValue)) {
            return;
        }
        writeBinaryValueSlow(buffer, meta, pValue);
    }

    private boolean writeBinaryValueFast(PacketWriter buffer, FieldMeta meta, long pValue) {
        DataType type = meta.getType();
        byte format = Value.getFormat(null, pValue);
        if (type.getSqlType() == Types.TINYINT) {
            if (format == Value.FORMAT_INT4) {
                buffer.writeByte((byte) Int4.get(pValue));
                return true;
            }
            else if (format == Value.FORMAT_INT8) {
                buffer.writeByte((byte) Int8.get(null, pValue));
                return true;
            }
        }
        else if (type.getJavaType() == Boolean.class) {
            boolean b = FishBool.get(null, pValue);
            buffer.writeByte((byte) (b ? 1 : 0));
            return true;
        }
        else if (type.getJavaType() == Integer.class) {
            if (format == Value.FORMAT_INT4) {
                buffer.writeUB4(Int4.get(pValue));
                return true;
            }
            else if (format == Value.FORMAT_INT8) {
                buffer.writeUB4((int) Int8.get(null, pValue));
                return true;
            }
        }
        else if (type.getJavaType() == Long.class) {
            if (format == Value.FORMAT_INT4) {
                buffer.writeLongLong(Int4.get(pValue));
                return true;
            }
            else if (format == Value.FORMAT_INT8) {
                buffer.writeLongLong(Int8.get(null, pValue));
                return true;
            }
        }
        else if (type.getJavaType() == Float.class) {
            if (format == Value.FORMAT_FLOAT4) {
                buffer.writeUB4(Float.floatToIntBits(Float4.get(null, pValue)));
                return true;
            }
        }
        else if (type.getJavaType() == Double.class) {
            if (format == Value.FORMAT_FLOAT4) {
                buffer.writeLongLong(Double.doubleToLongBits(Float8.get(null, pValue)));
                return true;
            }
        }
        else if (type.getJavaType() == String.class) {
            if (format == Value.FORMAT_UTF8 && getEncoder() == Charsets.UTF_8) {
                buffer.writeLenStringUtf8(pValue);
                return true;
            }
        }
        return false;
    }

    private void writeBinaryValueSlow(PacketWriter buffer, FieldMeta meta, long pValue) {
        Object value = FishObject.get(null, pValue);
        DataType type = meta.getType();
        if (type.getSqlType() == Types.TINYINT) {
            buffer.writeByte((byte) value);
        }
        else if (type.getJavaType() == Integer.class) {
            buffer.writeUB4((Integer) value);
        }
        else if (type.getJavaType() == Long.class) {
            buffer.writeLongLong((Long) value);
        }
        else if (type.getJavaType() == Float.class) {
            buffer.writeUB4(Float.floatToIntBits((Float) value));
        }
        else if (type.getJavaType() == Double.class) {
            buffer.writeLongLong(Double.doubleToLongBits((Double) value));
        }
        else if (type.getJavaType() == Timestamp.class) {
            buffer.writeTimestamp((Timestamp) value);
        }
        else if (type.getJavaType() == Date.class) {
            buffer.writeDate((Date) value);
        }
        else if (type.getJavaType() == byte[].class) {
            buffer.writeWithLength((byte[]) value);
        }
        else {
            buffer.writeLenString(value.toString(), getEncoder());
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
    public void writeRowTextBody(CursorMeta meta, PacketWriter buffer, long pRecord, int nColumns) {
        for (int i = 0; i < nColumns; i++) {
            Object fv = Record.getValue(pRecord, i);
            writeTextValue(buffer, fv, meta.getColumn(i));
        }
    }

    private void writeTextValue(PacketWriter buffer, Object fv, FieldMeta column) {
        DataType type = column.getType();
        if (fv instanceof Boolean) {
            // mysql has no boolean it is actually tinyint
            fv = ((Boolean) fv) ? 1 : 0;
        }
        if (fv == null) {
            // null mark is 251
            buffer.writeByte((byte) 251);
        }
        else if (fv instanceof Duration) {
            String text = UberFormatter.duration((Duration) fv);
            buffer.writeLenString(text, getEncoder());
        }
        else if (fv instanceof Timestamp) {
            // @see ResultSetRow#getDateFast, mysql jdbc driver only take precision 19,21,29 if callers wants
            // to get a Date from a datetime column
            Timestamp ts = (Timestamp) fv;
            if (ts.getTime() == Long.MIN_VALUE) {
                // special case for mysql '0000-00-00 00:00:00'
                buffer.writeLenString("0000-00-00 00:00:00", getEncoder());
            }
            else {
                String text;
                if (ts.getNanos() == 0) {
                    text = TIMESTAMP19_FORMAT.format(ts);
                }
                else {
                    text = TIMESTAMP29_FORMAT.format(ts);
                }
                buffer.writeLenString(text, getEncoder());
            }
        }
        else if (fv instanceof byte[]) {
            buffer.writeWithLength((byte[]) fv);
        }
        else if ((fv instanceof Date) && (((Date) fv).getTime() == Long.MIN_VALUE)) {
            // special case for mysql '0000-00-00'
            buffer.writeLenString("0000-00-00", getEncoder());
        }
        else if ((type.getJavaType() == BigDecimal.class) && (fv instanceof Double)) {
            BigDecimal bd = new BigDecimal((Double) fv);
            bd = bd.setScale(type.getScale(), RoundingMode.HALF_UP);
            buffer.writeLenString(bd.toPlainString(), getEncoder());
        }
        else if ((type.getJavaType() == BigDecimal.class) && (fv instanceof Float)) {
            BigDecimal bd = new BigDecimal((Float) fv);
            bd = bd.setScale(type.getScale(), RoundingMode.HALF_UP);
            buffer.writeLenString(bd.toPlainString(), getEncoder());
        }
        else if ((fv instanceof Integer) || (fv instanceof Long)) {
            String val = fv.toString();
            if (type.isZerofill()) {
                val = StringUtils.leftPad(val, type.getLength(), '0');
            }
            if (val.length() == 0) {
                // empty mark is 0
                buffer.writeByte((byte) 0);
            }
            else {
                buffer.writeLenString(val, getEncoder());
            }
        }
        else {
            String val = fv.toString();
            if (val.length() == 0) {
                // empty mark is 0
                buffer.writeByte((byte) 0);
            }
            else {
                buffer.writeLenString(val, getEncoder());
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
    public static void writeHandshakeBody(PacketWriter buffer, String serverVersion, byte protocolVersion,
            long threadId, int capability, byte charSet, int status, AuthPlugin plugin) {
        byte[] seed = plugin.getSeed();
        buffer.writeByte(protocolVersion);
        buffer.writeString(serverVersion);
        buffer.writeUB4(threadId);
        // seed part 1, first 8 bytes
        for (int i = 0; i < 8; i++) {
            buffer.writeByte(seed[i]);
        }
        buffer.writeByte((byte) 0);
        // lower 16 bits of sever capacity
        buffer.writeInt(capability);
        // serverCharsetIndex
        buffer.writeByte(charSet);
        // server status
        buffer.writeInt(status);
        // upper 16 bits of server capacity
        buffer.writeInt(capability >>> 16);
        // plugin data length
        if ((capability & MysqlServerHandler.CLIENT_PLUGIN_AUTH) != 0) {
            buffer.writeByte((byte) 0x15);
        }
        else {
            buffer.writeByte((byte) 0);
        }
        // fill the rest 10 bytes with 0
        buffer.writeLongLong(0);
        buffer.writeShort((short) 10);
        if ((capability & MysqlServerHandler.CLIENT_PLUGIN_AUTH) != 0) {
            for (int i = 8; i < seed.length; i++) {
                buffer.writeByte(seed[i]);
            }
            buffer.writeByte((byte) 0);
            buffer.writeString(plugin.getName());
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
    public void writeOKBody(PacketWriter buffer, long affectedRows, long insertId, String message, Session session) {
        // filed count
        buffer.writeByte((byte) 0);
        // affected rows
        buffer.writeLength(affectedRows);
        // inserId
        buffer.writeLength(insertId);
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
        buffer.writeUB2(status);
        // warning count = 0
        buffer.writeUB2(0);
        // message
        if (message != null) {
            buffer.writeLenString(message, Charsets.UTF_8);
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
    public void writeErrorBody(PacketWriter buffer, int errno, ByteBuffer message) {
        // field count
        buffer.writeByte((byte) 0xff);
        // error number
        buffer.writeUB2(errno);
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
    public void writeEOFBody(PacketWriter out, Session session) {
        // field count
        out.writeByte((byte) 0xfe);
        // warning count = 0
        out.writeUB2(0);
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
        out.writeUB2(status);
    }

    /**
     * 
     * Registers a slave at the master. Should be sent before requesting a binlog events with COM_BINLOG_DUMP.
     * 
     * <pre>
     * Bytes                        Name
     * -----                        ----
     *    1              [15] COM_REGISTER_SLAVE
     *    4              server-id
     *    1              slaves hostname length
     *    string[$len]   slaves hostname
     *    1              slaves user len
     *    string[$len]   slaves user
     *    1              slaves password len
     *    string[$len]   slaves password
     *    2              slaves mysql-port
     *    4              replication rank
     *    4              master-id
     * 
     * @see https://dev.mysql.com/doc/internals/en/com-register-slave.html
     * </pre>
     * 
     */
    public void writeRegisterSlave(PacketWriter buffer, int serverId) {
        // code for COM_REGISTER_SLAVE is 0x15
        buffer.writeByte((byte) 0x15);
        buffer.writeUB4(serverId);
        // usually empty
        buffer.writeLenString("", Charsets.UTF_8);
        // usually empty
        buffer.writeLenString("", Charsets.UTF_8);
        // usually empty
        buffer.writeLenString("", Charsets.UTF_8);
        // usually empty
        buffer.writeUB2(0);
        // replication rank to be ignored
        buffer.writeBytes(new byte[4]);
        // master id, usually 0
        buffer.writeUB4(0);
    }

    /**
     * 
     * Requests a binlog network stream from the master starting a given position.
     * 
     * <pre>
     * Bytes                        Name
     * -----                        ----
     *    1              [12] COM_BINLOG_DUMP
     *    4              binlog-pos
     *    2              flags
     *    4              server-id
     *    string[EOF]    binlog-filename
     * 
     * @see https://dev.mysql.com/doc/internals/en/com-binlog-dump.html
     * </pre>
     * 
     */
    public void writeBinlogDump(PacketWriter buffer, long binlogPos, int serverId, String binlogName) {
        // code for COM_BINLOG_DUMP is 0x12
        buffer.writeByte((byte) 0x12);
        buffer.writeUB4(binlogPos);
        // flag, 0 means don't send EOF if no more bing log
        buffer.writeUB2(0);
        // server id of this slave
        buffer.writeUB4(serverId);
        // binlog file name
        buffer.writeString(binlogName);
    }

    /**
     * 
     * Handshake responce for replication master.
     * 
     * <pre>
     * Bytes                        Name
     * -----                        ----
     *    2              capability flags, CLIENT_PROTOCOL_41 never set
     *    3              max-packet size
     *    string[NUL]    username
     *    string[NUL]    auth-response
     *    string[NUL]    database
     * 
     * @see https://dev.mysql.com/doc/internals/en/com-binlog-dump.html
     * </pre>
     * 
     */
    public void writeHandshakeResponse(PacketWriter buf, int capability, String user, String password) {
        // capability flags
        buf.writeInt(42117);
        // max-packet size, 0?
        buf.writeLongInt(0);
        buf.writeString(user);
        buf.writeString(password);
        // default dbname
        buf.writeString("");
    }

    private Charset getEncoder() {
        return this.cs.get();
    }

    private Charset getMetadataEncoder() {
        return this.metacs.get();
    }

    /*
     * this ony works in mariadb https://mariadb.com/kb/en/library/err_packet/
     */
    public void writeProgressBody(PacketWriter buffer, String result, Session session) {
        // header
        buffer.writeByte((byte) 0xff);
        // error code
        buffer.writeUB2(0xffff);
        // stage
        buffer.writeByte((byte) 1);
        // max stage
        buffer.writeByte((byte) 11);
        // progress
        buffer.writeLongInt(1);
        // message
        buffer.writeLenString(result, Charsets.UTF_8);
    }

    public void writeRow(PacketWriter buffer, Row row) {
        int len = row.getLength();
        buffer.writeBytes(row.getAddress(), len);
    }

    /*
     * @see https://mariadb.com/kb/en/library/connection/
     */
    public void writeLogin(PacketWriter buffer, String user, String password, String dbname) {
        // int4, capabilities
        long capabilities = CLIENT_PROTOCOL_41 | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
        if (dbname != null) {
            capabilities |= CLIENT_CONNECT_WITH_DB;
        }
        buffer.writeLong(capabilities);
        // int4, max packet size - 16m
        buffer.writeLong(0xffffff);
        // int1, charset
        buffer.writeByte((byte) MYSQL_COLLATION_INDEX_utf8_general_ci);
        // 19 bytes reserved
        for (int i = 0; i < 19; i++) {
            buffer.writeByte((byte) 0);
        }
        ;
        // int4, extended capabilitis or reserved
        buffer.writeLong(0);
        // user name
        buffer.writeString(user);
        // password
        buffer.writeByte((byte) 1);
        buffer.writeByte((byte) 1);
        // dbname
        if (dbname != null) {
            buffer.writeString(dbname);
        }
        // plugin
    }

    public void writeBackup(PacketWriter buffer, String fullname) {
        buffer.writeByte((byte) PacketType.FISH_BACKUP.getId());
        buffer.writeString(fullname);
    }

    public void close() {
        this.packet.close();
    }

    public void writeRowTextBodyResult(ResultSetMetaData meta, PacketWriter buffer, ResultSet pRecord, int nColumns) {
        for (int i = 1; i <= nColumns; i++) {
            try {
                Object fv = pRecord.getObject(i);
                String className = meta.getColumnClassName(i);
                int scale = meta.getScale(i);
                writeTextValueResult(buffer, fv, Class.forName(className), scale);
            }
            catch (Exception e) {
                _log.warn(e.getMessage(),e);
                throw new OrcaObjectStoreException(e)  ;
            }
        }
    }

    private void writeTextValueResult(PacketWriter buffer, Object fv, Class<?> type, int scale) throws SQLException {

        if (fv instanceof Boolean) {
            // mysql has no boolean it is actually tinyint
            fv = ((Boolean) fv) ? 1 : 0;
        }
        if (fv == null) {
            // null mark is 251
            buffer.writeByte((byte) 251);
        }
        else if (fv instanceof Duration) {
            String text = UberFormatter.duration((Duration) fv);
            buffer.writeLenString(text, getEncoder());
        }
        else if (fv instanceof Timestamp) {
            // @see ResultSetRow#getDateFast, mysql jdbc driver only take precision 19,21,29 if callers wants
            // to get a Date from a datetime column
            Timestamp ts = (Timestamp) fv;
            if (ts.getTime() == Long.MIN_VALUE) {
                // special case for mysql '0000-00-00 00:00:00'
                buffer.writeLenString("0000-00-00 00:00:00", getEncoder());
            }
            else {
                String text;
                if (ts.getNanos() == 0) {
                    text = TIMESTAMP19_FORMAT.format(ts);
                }
                else {
                    text = TIMESTAMP29_FORMAT.format(ts);
                }
                buffer.writeLenString(text, getEncoder());
            }
        }
        else if (fv instanceof byte[]) {
            buffer.writeWithLength((byte[]) fv);
        }
        else if ((fv instanceof Date) && (((Date) fv).getTime() == Long.MIN_VALUE)) {
            buffer.writeLenString("0000-00-00", getEncoder());
        }
        else if ((type == BigDecimal.class) && (fv instanceof BigDecimal)) {
            BigDecimal bd =  (BigDecimal) fv;
            bd = bd.setScale(scale, RoundingMode.HALF_UP);
            buffer.writeLenString(bd.toPlainString(), getEncoder());
        }
        else if ((type == BigDecimal.class) && (fv instanceof Double)) {
            BigDecimal bd = new BigDecimal((Double) fv);
            bd = bd.setScale(scale, RoundingMode.HALF_UP);
            buffer.writeLenString(bd.toPlainString(), getEncoder());
        }
        else if ((type == BigDecimal.class) && (fv instanceof Float)) {
            BigDecimal bd = new BigDecimal((Float) fv);
            bd = bd.setScale(scale, RoundingMode.HALF_UP);
            buffer.writeLenString(bd.toPlainString(), getEncoder());
        }
        else if ((fv instanceof Integer) || (fv instanceof Long)) {
            String val = fv.toString();
            if (val.length() == 0) {
                // empty mark is 0
                buffer.writeByte((byte) 0);
            }
            else {
                buffer.writeLenString(val, getEncoder());
            }
        }
        else {
            String val = fv.toString();
            if (val.length() == 0) {
                // empty mark is 0
                buffer.writeByte((byte) 0);
            }
            else {
                buffer.writeLenString(val, getEncoder());
            }
        }
    }

    public void writeRowTextBodyGroup( PacketWriter buffer, Group group) {
        GroupType groupType = group.getType();
        List<Type> fields = groupType.getFields();
        for (Type field : fields) {
            String columnName = field.getName();
            if (columnName.startsWith("*")) {
                continue;
            }
            Type type = group.getType().getType(columnName);
            
            PrimitiveType.PrimitiveTypeName typeStr = type.asPrimitiveType()
                    .getPrimitiveTypeName();
            OriginalType originalType = type.getOriginalType();
            int volumnValCount = group.getFieldRepetitionCount(columnName);
            if (volumnValCount <= 0) {
                buffer.writeByte((byte) 0);
                continue;
            }
            if (typeStr == PrimitiveTypeName.BINARY) {
                Binary binaryVal = group.getBinary(columnName, 0);
                if (binaryVal != null) {
                    byte[] colValueBytes = binaryVal.getBytes();
                    if (colValueBytes == null || colValueBytes.length == 0) {
                        buffer.writeByte((byte) 0);
                        continue;
                    }
                    String str = new String(colValueBytes);
                    if (originalType == OriginalType.DECIMAL) {
                        buffer.writeLenString(str, getEncoder());
                    }
                    else if (originalType == OriginalType.DATE) {
                        Long lval = Long.parseLong(str);
                        Date val = new Date(lval);
                        val.setTime(lval);
                        buffer.writeDate(val);
                    }
                    else if (originalType == OriginalType.TIME_MILLIS) {
                        Long lval = Long.parseLong(str);
                        Time val = new Time(lval);
                        buffer.writeLenString(TIME_FORMAT.format(val), getEncoder());
                    }
                    else if (originalType == OriginalType.TIMESTAMP_MILLIS) {
                        Long lval = Long.parseLong(str);
                        Timestamp val = new Timestamp(lval);
                        buffer.writeTimestamp(val);
                    }
                    else if (originalType == OriginalType.INT_32) {
                        buffer.writeLenString(str, getEncoder());
                    }
                    else if (originalType == OriginalType.INT_64) {
                        buffer.writeLenString(str, getEncoder());
                    }
                    else if (originalType == OriginalType.UTF8) {
                        buffer.writeLenString(str, getEncoder());
                    }
                    else {
                        buffer.writeWithLength(binaryVal.getBytes());
                    }
                }
            }
            else if (typeStr == PrimitiveType.PrimitiveTypeName.INT64) {
                Long lval = group.getLong(columnName, 0);
                if (originalType == OriginalType.DATE) {
                    Date val = new Date(lval);
                    val.setTime(lval);
                    buffer.writeDate(val);
                }
                else if (originalType == OriginalType.TIME_MILLIS) {
                    Time val = new Time(lval);
                    buffer.writeLenString(TIME_FORMAT.format(val), getEncoder());
                }
                else if (originalType == OriginalType.TIMESTAMP_MILLIS) {
                    Timestamp val = new Timestamp(lval);
                    buffer.writeTimestamp(val);
                }
                else {
                    buffer.writeLenString(String.valueOf(lval), getEncoder());
                }
            }
            else if (typeStr == PrimitiveType.PrimitiveTypeName.BOOLEAN) {
                int fv = group.getBoolean(columnName, 0) ? 1 : 0;
                String val = fv + "";
                buffer.writeLenString(val, getEncoder());

            }
            else if (typeStr == PrimitiveType.PrimitiveTypeName.INT32) {
                String val = group.getLong(columnName, 0) + "";
                if (val.length() == 0) {
                    buffer.writeByte((byte) 0);
                }
                else {
                    buffer.writeLenString(val, getEncoder());
                }
            }
            else if (typeStr == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) { 
                Binary binaryVal = group.getBinary(columnName, 0);
                if (binaryVal != null) {
                    byte[] colValueBytes = binaryVal.getBytes();
                    if (colValueBytes == null || colValueBytes.length == 0) {
                        buffer.writeByte((byte) 0);
                        continue;
                    }
                   
                    if (originalType == OriginalType.DECIMAL) {
                        DecimalMetadata metadata = type
                                .asPrimitiveType()
                                .getDecimalMetadata();
                        int precision =  metadata.getPrecision();
                        int scale = metadata.getScale();
                        BigDecimal decimalValue = com.antsdb.saltedfish.parquet.Helper.binaryToDecimal(
                                            binaryVal,
                                            precision,
                                            scale);
                        
                        buffer.writeLenString(decimalValue.stripTrailingZeros().toPlainString(), getEncoder());
                    }
                    else {
                        buffer.writeWithLength(binaryVal.getBytes());
                    }
                }
            }
            else {
                throw new NotImplementedException();
            }
        }
    }
}