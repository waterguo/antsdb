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

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.StorageEngine;
import com.antsdb.saltedfish.obs.ExternalQuerySession;
import com.antsdb.saltedfish.obs.ObsProvider;
import com.antsdb.saltedfish.parquet.ObsService;
import com.antsdb.saltedfish.parquet.OrcaObjectStoreException;
import com.antsdb.saltedfish.parquet.ParquetDataReader;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author lizc
 */
class HelperExternal {
    static Logger _log = UberUtil.getThisLogger();

    static void writeRows(ChannelWriter out, Session session, PacketEncoder encoder, ResultSet result)
            throws SQLException, ClassNotFoundException {
        ResultSetMetaData metaData = result.getMetaData();
        int nColumns = metaData.getColumnCount();
        while (result.next()) {
            encoder.writePacket(out, (packet) -> encoder.writeRowTextBodyResult(metaData, packet, result, nColumns));
        }
        // end row
        encoder.writePacket(out, (packet) -> encoder.writeEOFBody(packet, session));
    }

    static void writeExternalResonpse(Humpback humpback, ChannelWriter out, MysqlSession mysession, Object result) {
        if (result == null) {
            mysession.encoder.writePacket(out, (packet) -> mysession.encoder.writeOKBody(packet, 0,
                    mysession.session.getLastInsertId(), null, mysession.session));
        }
        else if (result instanceof ResultSet) {
            mysession.session.fetch((ResultSet) result, () -> {
                try {
                    HelperExternal.writeResultSet(out, mysession.session, mysession.encoder, (ResultSet) result);
                }
                catch (Exception e) {
                    throw new OrcaException("exec sql write resonpse error, {}", e);
                }
            });
        }
        else if (result instanceof String) {
            String resultPath = (String) result;
            StorageEngine stor = humpback.getStorageEngine0();
            ObsService service = null;
            if (stor instanceof ObsService) {
                service = (ObsService) stor;
            }
            else {
                throw new OrcaObjectStoreException("obs service error");
            }
            ObsProvider client = service.getStoreClient();
            try {
                Configuration config = ExternalQuerySession.getInstance().getHdfsConfig();
                List<String> datafiles = client.listFiles(resultPath, null,".parquet");
                boolean writeHeader = true;
                if (datafiles == null || datafiles.size() == 0) {
                    mysession.encoder.writePacket(out, (packet) -> mysession.encoder.writeOKBody(packet, 0,
                            mysession.session.getLastInsertId(), null, mysession.session));
                }
                else {
                    for (String file : datafiles) {
                        _log.debug("spark sql result data file:{}",file);
                        HelperExternal.writeGroup(out, mysession.session, mysession.encoder, file, writeHeader, config);
                        writeHeader = false;
                    }
                }
            }
            catch (Exception e) {
                throw new OrcaException("exec sql write resonpse error, {}", e);
            }
            finally {
                try {
                    if (resultPath.startsWith("hdfs:") || resultPath.startsWith("file:")) {
                        client.deleteObject(resultPath);
                    }
                    else {
                        File resultDir = new File(resultPath);
                        if (resultDir.exists()) {
                            FileUtils.deleteDirectory(resultDir);
                        }
                    }
                }
                catch (Exception e) {
                    throw new OrcaException("exec sql write resonpse error, {}", e);
                }
            }
        }
        else {
            mysession.out.write(PacketEncoder.OK_PACKET);
        }
    }

    static void writeResultSet(ChannelWriter out, Session session, PacketEncoder encoder, ResultSet result)
            throws SQLException, ClassNotFoundException {
        try (ResultSet cursor = (ResultSet) result) {
            writeMeta(out, session, encoder, cursor);
            writeRows(out, session, encoder, cursor);
        }
    }

    public static void writeMeta(ChannelWriter out, Session session, PacketEncoder encoder, ResultSet cursor)
            throws SQLException, ClassNotFoundException {
        int nColumns = cursor.getMetaData().getColumnCount();
        encoder.writePacket(out, (packet) -> encoder.writeResultSetHeaderBody(packet, nColumns));
        // write parameter field packet
        for (int i = 1; i <= nColumns; i++) {
            ResultSetMetaData metaData = cursor.getMetaData();
            String columnName = metaData.getColumnName(i);
            String className = metaData.getColumnClassName(i);
            String typeName = metaData.getColumnTypeName(i);
            int sqlType = metaData.getColumnType(i);
            DataType type = new DataType(typeName, metaData.getPrecision(i), metaData.getScale(i), sqlType,
                    Class.forName(className), (byte) 1, 1);
            FieldMeta column = new FieldMeta(columnName, type);
            column.setSourceColumnName(columnName);
            ObjectName sourceTableName = new ObjectName(metaData.getSchemaName(i), metaData.getTableName(i));
            column.setSourceTable(sourceTableName);
            column.setTableAlias(metaData.getTableName(i));
            encoder.writePacket(out, (packet) -> encoder.writeColumnDefBody(packet, column));
        }
        encoder.writePacket(out, (packet) -> encoder.writeEOFBody(packet, session));
    }

    private static void writeGroup(
            ChannelWriter out, 
            Session session, 
            PacketEncoder encoder, 
            String datafile,
            boolean writeHeader, 
            Configuration conf) {
        ParquetDataReader reader = new ParquetDataReader(new Path(datafile), conf);
        try {
            if (reader != null) {
                try {
                    if (writeHeader) {
                        writeGroupMeta(out, session, encoder, reader.getSchema());
                    }
                    writeGroupRows(out, session, encoder, reader);
                }
                catch (Exception e) {
                    _log.warn("out sparksql result error,error msg:{}",e.getMessage(),e);
                }
            }
        }
        catch (Exception e) {
            _log.warn("read sparksql result error,error msg:{}",e.getMessage(),e);
        }
        finally {
            if (reader != null) {
                try {
                    reader.close();
                }
                catch (Exception e) {
                    _log.warn(e.getMessage(),e);
                }
            }
        }
    }

    private static void writeGroupRows(
            ChannelWriter out, 
            Session session, 
            PacketEncoder encoder,
            ParquetDataReader reader
            ) throws IOException {
        Group result = null;
        while ((result = reader.readNext()) != null) {
            final Group tmp = result;
            encoder.writePacket(out, (packet) -> encoder.writeRowTextBodyGroup(packet, tmp));
        }
        // end row
        encoder.writePacket(out, (packet) -> encoder.writeEOFBody(packet, session));
    }

    private static void writeGroupMeta(ChannelWriter out, Session session, PacketEncoder encoder, MessageType schema)
            throws ClassNotFoundException {
        int tmp = 0;// schema.getFieldCount();
        for (Type metaData : schema.getFields()) {
            if (!metaData.getName().startsWith("*")) {
                tmp++;
            }
        }
        int nColumns = tmp;
        encoder.writePacket(out, (packet) -> encoder.writeResultSetHeaderBody(packet, nColumns));
        // write parameter field packet
        for (Type metaData : schema.getFields()) {
            PrimitiveType ptype = schema.getType(metaData.getName()).asPrimitiveType();// .getPrimitiveTypeName();
            String columnName = metaData.getName();
            if (metaData.getName().startsWith("*")) {
                continue;
            }
            String className = metaData.getClass().getName();
            String typeName = metaData.getName();
            OriginalType originalType = schema.getType(metaData.getName()).getOriginalType();

            int sqlType = 0;// metaData.getOriginalType();
            if (ptype.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY) {
                sqlType = Types.BINARY;
                if (originalType == OriginalType.DECIMAL) {
                    sqlType = Types.DECIMAL;
                }
                else if (originalType == OriginalType.DATE) {
                    sqlType = Types.DATE;
                }
                else if (originalType == OriginalType.TIME_MILLIS) {
                    sqlType = Types.TIME;
                }
                else if (originalType == OriginalType.TIMESTAMP_MILLIS) {
                    sqlType = Types.TIMESTAMP;
                }
                else if (originalType == OriginalType.INT_32) {
                    sqlType = Types.INTEGER;
                }
                else if (originalType == OriginalType.INT_64) {
                    sqlType = Types.BIGINT;
                }
                else if (originalType == OriginalType.UTF8) {
                    sqlType = Types.VARCHAR;
                }
                else {
                    sqlType = Types.BINARY;
                }
            }
            else if (ptype.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT64) {
                sqlType = Types.BIGINT;
            }
            else if (ptype.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT32) {
                sqlType = Types.INTEGER;
            }
            else if (ptype.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BOOLEAN) {
                sqlType = Types.BOOLEAN;
            }
            else if (ptype.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.FLOAT) {
                sqlType = Types.FLOAT;
            }
            else if (ptype.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.DOUBLE) {
                sqlType = Types.DOUBLE;
            }

            DataType type = new DataType(typeName, 256, 2, sqlType, Class.forName(className), (byte) 1, 1);
            FieldMeta column = new FieldMeta(columnName, type);
            column.setSourceColumnName(columnName);
            ObjectName sourceTableName = new ObjectName(metaData.getName(), metaData.getName());
            column.setSourceTable(sourceTableName);
            column.setTableAlias(metaData.getName());
            encoder.writePacket(out, (packet) -> encoder.writeColumnDefBody(packet, column));
        }
        encoder.writePacket(out, (packet) -> encoder.writeEOFBody(packet, session));

    }

}
