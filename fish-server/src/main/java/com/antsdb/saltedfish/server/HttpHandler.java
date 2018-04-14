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
package com.antsdb.saltedfish.server;

import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.Record;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.AsciiString;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.*;

import java.sql.SQLException;
import java.util.function.Supplier;

/**
 * 
 * @author *-xguo0<@
 */
public class HttpHandler extends ChannelInboundHandlerAdapter  {

    private Orca orca;

    private static final AsciiString CONTENT_TYPE = AsciiString.of("Content-Type");
    private static final AsciiString CONTENT_LENGTH = AsciiString.of("Content-Length");
    private static final AsciiString CONNECTION = AsciiString.of("Connection");
    private static final AsciiString KEEP_ALIVE = AsciiString.of("keep-alive");

    public HttpHandler(Orca orca) {
        this.orca = orca;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            if (msg instanceof HttpRequest) {
                HttpRequest req = (HttpRequest) msg;
    
                boolean keepAlive = HttpUtil.isKeepAlive(req);
                String content = getContent();
                FullHttpResponse response = new DefaultFullHttpResponse(
                        HTTP_1_1, 
                        OK, 
                        Unpooled.wrappedBuffer(content.getBytes()));
                response.headers().set(CONTENT_TYPE, "text/html");
                response.headers().setInt(CONTENT_LENGTH, response.content().readableBytes());
    
                if (!keepAlive) {
                    ctx.write(response).addListener(ChannelFutureListener.CLOSE);
                } 
                else {
                    response.headers().set(CONNECTION, KEEP_ALIVE);
                    ctx.write(response);
                }
            }
        }
        catch (Exception x) {
            x.printStackTrace();
            FullHttpResponse response = new DefaultFullHttpResponse(
                    HTTP_1_1, 
                    OK, 
                    Unpooled.wrappedBuffer(x.getMessage().getBytes()));
            response.headers().set(CONTENT_TYPE, "text/text");
            response.headers().setInt(CONTENT_LENGTH, response.content().readableBytes());
            ctx.write(response).addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
    
    private String getContent() throws SQLException {
        StringBuilder buf = new StringBuilder();
        buf.append("<html><body style='font-family:Verdana;'>");
        writeTable(buf, "System Information", "antsdb.system_info");
        writeTable(buf, "Cache Information", "antsdb.cache_info");
        writeTable(buf, "Minke Information", "antsdb.minke_info");
        writeTable(buf, "Replicator Information", "antsdb.replicator_info");
        writeTable(buf, "Synchronizer Information", "antsdb.synchronizer_info");
        writeTable(buf, "HBase Information", "antsdb.hbase");
        buf.append("</body></html>");
        return buf.toString();
    }

    private void writeTable(StringBuilder buf, String title, String string) throws SQLException {
        Session session = this.orca.createSystemSession();
        try {
            buf.append("<h1>");
            buf.append(title);
            buf.append("</h1>");
            buf.append("<table border='1' style='border-collapse:collapse;font-family:Verdana;'>");
            String sql = "SELECT * FROM " + string;
            try (Cursor c = (Cursor)session.run(sql)) {
                if (c != null) {
                    for (;;) {
                        long pRecord = c.next();
                        if (pRecord == 0) {
                            break;
                        }
                        Record rec = Record.toRecord(pRecord);
                        writeTr(buf, ()->{
                            buf.append("<tr>");
                            writeTd(buf, rec.get(0));
                            writeTd(buf, rec.get(1));
                            buf.append("</tr>");
                            return null;
                        });
                    }
                }
            }
            buf.append("</table>");
        }
        finally {
            session.close();
        }
    }

    private void writeTr(StringBuilder buf, Supplier<Integer> callback) {
        buf.append("<tr>");
        callback.get();
        buf.append("</tr>");
    }
    
    private void writeTd(StringBuilder buf, Object value) {
        buf.append("<td>");
        buf.append(value==null ? "null" : value.toString());
        buf.append("</td>");
    }
}
