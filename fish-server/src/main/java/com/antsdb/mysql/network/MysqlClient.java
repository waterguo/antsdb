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

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;

import org.apache.commons.codec.Charsets;

import com.antsdb.saltedfish.cpp.AllocPoint;
import com.antsdb.saltedfish.cpp.MemoryManager;
import com.antsdb.saltedfish.server.mysql.ChannelWriterNio;
import com.antsdb.saltedfish.server.mysql.PacketEncoder;

/**
 * 
 * @author *-xguo0<@
 */
public class MysqlClient {
    private String host;
    private int port;
    private SocketChannel ch;
    private PacketEncoder encoder;
    private ChannelWriterNio out;
    private ByteBuffer buf = MemoryManager.allocImmortal(AllocPoint.MYSQL_CLIENT, 1024);
    
    public MysqlClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.buf.order(ByteOrder.LITTLE_ENDIAN);
    }
    
    public PacketHandshake connect() throws IOException {
        this.ch = SocketChannel.open(new InetSocketAddress(host, port));
        this.out = new ChannelWriterNio(this.ch);
        this.encoder = new PacketEncoder(()->{ return Charsets.UTF_8;});
        readPacket();
        PacketHandshake packet = new PacketHandshake(this.buf);
        return packet;
    }
    
    public void close() {
        MemoryManager.freeImmortal(AllocPoint.MYSQL_CLIENT, this.buf);
    }
    
    public void login(String user, String password) throws Exception {
        this.encoder.writePacket(this.out, (packet)->{
            this.encoder.writeLogin(packet, user, password, null);
        });
        this.out.flush();
        readPacket();
        if (isOk(this.buf)) {
            return;
        }
        else {
            PacketError error = new PacketError(this.buf);
            throw new Exception(error.getErrorMessage());
        }
    }

    private boolean isOk(ByteBuffer packet) {
        return packet.get(4) == 0;
    }

    public boolean isEof(ByteBuffer packet) {
        return (packet.get(4) & 0xff) == 0xfe;
    }
    
    public boolean isError(ByteBuffer packet) {
        return (packet.get(4) & 0xff) == 0xff;
    }
    
    public ByteBuffer readPacket() throws IOException {
        this.buf.clear();
        readHeader();
        readBody();
        this.buf.flip();
        return this.buf;
    }
    
    public ByteBuffer readPacketErrorCheck() throws Exception {
        ByteBuffer packet = readPacket();
        if (isError(packet)) {
            PacketError error = new PacketError(this.buf);
            throw new Exception(error.getErrorMessage());
        }
        return packet;
    }
    
    private void readHeader() throws IOException {
        this.buf.limit(4);
        for(;;) {
            int nread = this.ch.read(this.buf);
            if (nread < 0) {
                throw new EOFException();
            }
            if (this.buf.position() < 4) {
                // not enough bytes for the header
                continue;
            }
            break;
        }
    }

    private void readBody() throws IOException {
        int size = this.buf.getInt(0) & 0xffffff;
        if (size == 0) {
            throw new IllegalArgumentException();
        }
        int packetSize = size + 4;
        if (buf.capacity() < packetSize) {
            // grow the buffer size
            this.buf = MemoryManager.growImmortal(AllocPoint.MYSQL_CLIENT, this.buf, packetSize);
        }
        buf.limit(packetSize);
        for(;;) {
            int nread = this.ch.read(this.buf);
            if (nread < 0) {
                // connection closed
                throw new EOFException();
            }
            if (this.buf.position() < size + 4) {
                // not enough bytes for the body
                continue;
            }
            break;
        }
    }
    
    public void backup(String fullname) {
        this.encoder.writePacket(this.out, (packet)->{
            this.encoder.writeBackup(packet, fullname);
        });
        this.out.flush();
    }

}
