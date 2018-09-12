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

import java.nio.ByteBuffer;

import com.antsdb.saltedfish.sql.FinalCursor;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;

/**
 * 
 * @author wgu0
 */
class Helper {
    public static void writeCursorMeta(ChannelWriter out, Session session, PacketEncoder encoder, Cursor cursor) {
        int nColumns = getColumnCount(cursor.getMetadata());
        encoder.writePacket(
                out,
                (packet) -> encoder.writeResultSetHeaderBody(
                        packet, 
                        nColumns));

        // write parameter field packet

        for (int i=0; i<nColumns; i++) {
            FieldMeta column = cursor.getMetadata().getColumn(i);
            encoder.writePacket(out, (packet) -> encoder.writeColumnDefBody(packet, column));
        }

        encoder.writePacket(out, (packet) -> encoder.writeEOFBody(packet, session));
    }

    static void writeRows(ChannelWriter out, Session session, PacketEncoder encoder, Cursor cursor, boolean text) {
        int nColumns = getColumnCount(cursor.getMetadata());
        for (;;) {
            long pRecord = cursor.next();
            if (pRecord == 0) {
                break;
            }
            if (text) {
                encoder.writePacket(
                        out,
                        (packet) -> encoder.writeRowTextBody(
                            cursor.getMetadata(),
                            packet, 
                            pRecord, 
                            nColumns));
            }
            else {
                encoder.writePacket(
                        out,
                        (packet) -> encoder.writeRowBinaryBody(
                            packet, 
                            pRecord, 
                            cursor.getMetadata(), 
                            nColumns));
            }
        }

        // end row
        encoder.writePacket(out, (packet) -> encoder.writeEOFBody(packet, session));
    }
    
    static void writeCursor(ChannelWriter out, 
                            MysqlSession mysession, 
                            Cursor result, 
                            ByteBuffer meta, 
                            boolean text) {
        try (Cursor cursor = (Cursor) result) {
            mysession.out.write(meta);
            writeRows(out, mysession.session, mysession.encoder, cursor, text);
        }
    }
    
    static void writeCursor(ChannelWriter out, Session session, PacketEncoder encoder, Cursor result, boolean text) {
        try (Cursor cursor = (Cursor) result) {
            writeCursorMeta(out, session, encoder, cursor);
            writeRows(out, session, encoder, cursor, text);
        }
    }

    private static int getColumnCount(CursorMeta metadata) {
        // skip system columns , the ones starts with "*"
        int j = 0;
        for (FieldMeta i: metadata.getColumns()) {
            if (i.getName().startsWith("*")) {
                break;
            }
            j++;
        }
        return j;
    }

    static void writeResonpse(ChannelWriter out, MysqlSession mysession, Object result, boolean text) {
        if (result==null){
            mysession.encoder.writePacket(
                    out,
                    (packet) -> mysession.encoder.writeOKBody(
                            packet, 
                            0, 
                            mysession.session.getLastInsertId(),
                            null,
                            mysession.session));
        }
        else if (result instanceof Cursor) {
            mysession.session.fetch((FinalCursor)result, () -> {
                Helper.writeCursor(out, mysession.session, mysession.encoder, (Cursor)result, text);
            });
        }
        else if (result instanceof Integer) {
            Integer count = (Integer) result;
            mysession.encoder.writePacket(
                    out,
                    (packet) -> mysession.encoder.writeOKBody(
                            packet, 
                            count, 
                            mysession.session.getLastInsertId(),
                            null,
                            mysession.session));
        }
        else if (result instanceof String) {
            mysession.encoder.writePacket(
                    out,
                    (packet) -> mysession.encoder.writeProgressBody(
                            packet, 
                            (String)result,
                            mysession.session));
        }
        else {
            mysession.out.write(PacketEncoder.OK_PACKET);
        }
    }
}
