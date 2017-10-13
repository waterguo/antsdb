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

import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.CheckPoint;
import com.antsdb.saltedfish.server.SaltedFish;
import com.antsdb.saltedfish.server.mysql.packet.replication.GenericPacket;
import com.antsdb.saltedfish.server.mysql.packet.replication.ReplicationPacket;
import com.antsdb.saltedfish.server.mysql.packet.replication.RotatePacket;
import com.antsdb.saltedfish.server.mysql.packet.replication.RowsEventV2Packet;
import com.antsdb.saltedfish.server.mysql.packet.replication.StateIndicator;
import com.antsdb.saltedfish.server.mysql.packet.replication.StopPacket;
import com.antsdb.saltedfish.server.mysql.packet.replication.TableMapPacket;
import com.antsdb.saltedfish.server.mysql.packet.replication.XIDPacket;
import com.antsdb.saltedfish.server.mysql.util.BindValue;
import com.antsdb.saltedfish.server.mysql.util.BindValueUtil;
import com.antsdb.saltedfish.server.mysql.util.ReplicatedRow;
import com.antsdb.saltedfish.sql.ConfigService;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.util.UberUtil;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import static com.antsdb.saltedfish.server.mysql.MysqlServerHandler.*;

public class MysqlClientHandler extends ChannelInboundHandlerAdapter 
{
    static Logger _log = UberUtil.getThisLogger();

    SaltedFish fish;
    Session session;
    
    TableMapPacket currentTableMap;

    public String masterUser ;
    public String masterPassword ;
    public String masterBinlog ;
    public long masterLogPos;

    public String currentBinlog ;

    public long currentPos = -1;

    public PacketEncoder packetEncoder;
    
    public MysqlClientHandler() {
        this.fish = SaltedFish.getInstance();
        masterUser = getConfig().getSlaveUser();
        masterPassword = getConfig().getSlavePassword();
        masterBinlog = getCheckPoint().getSlaveLogFile();
        masterLogPos = getCheckPoint().getSlaveLogPosition();
    	packetEncoder = new PacketEncoder();
    }

    @Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    	if (this.session != null) {
    		this.fish.getOrca().closeSession(this.session);
    	}
        _log.info("Replication slave closed.");
		
		super.channelInactive(ctx);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (_log.isTraceEnabled()) {
            _log.debug(msg.toString());
        }
        
        // process the request with error handling
        
        try {
            run(ctx, msg);
        }
        catch (Exception x) {
            if (_log.isDebugEnabled()) {
                _log.error("error detail: \n {}", msg, x);
            }
        }
	}

    private void run(ChannelHandlerContext ctx, Object msg) throws Exception {
    	if (msg instanceof StateIndicator) {
        	state(ctx, (StateIndicator)msg);
        }
        else if (msg instanceof RotatePacket) {
        	RotatePacket pkt = (RotatePacket)msg;
        	currentBinlog = pkt.binlog;
        	_log.info("Binlog file:" + currentBinlog);
        	currentPos = pkt.nextPosition;
        	getCheckPoint().setSlaveLogFile(currentBinlog);
        }
        else if (msg instanceof TableMapPacket) {
        	currentTableMap = (TableMapPacket)msg;
        	_log.trace("TableMapPacket:" + currentTableMap.schemaName + "." + currentTableMap.tableName);
        	currentPos = currentTableMap.nextPosition;
        }
        else if (msg instanceof RowsEventV2Packet) {
        	RowsEventV2Packet pkt = (RowsEventV2Packet)msg;
        	_log.trace("RowsEventV2Packet:");
    		processRows(currentTableMap, pkt);
        	currentPos = pkt.nextPosition;
        }
        else if (msg instanceof XIDPacket) {
        	XIDPacket pkt = (XIDPacket)msg;
        	_log.trace("XIDPacket:" + pkt.xid);
        	currentPos = pkt.nextPosition;
        }
        else if (msg instanceof StopPacket) {
        	StopPacket pkt = (StopPacket)msg;
        	// should we close or keep going?
        	//	close(ctx);
        	_log.trace("StopPacket");
        	currentPos = pkt.nextPosition;
        }
        else if (msg instanceof GenericPacket) {
        	GenericPacket pkt = (GenericPacket)msg;
        	currentPos = pkt.nextPosition;
        	_log.trace("GenericPacket type: {}", ((GenericPacket)msg).eventType);
        }
        else {
            _log.warn("unknown event type: {}", msg.getClass().toString());
        }
    	
    	getCheckPoint().setSlaveLogPosition(this.currentPos);
    	_log.trace("Binlog file position: {}", currentPos);
    }

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable x) throws Exception {
	    _log.error(x.getMessage());
	}

	public void processRows(TableMapPacket map, RowsEventV2Packet msg)
	{
		List<ReplicatedRow> rows = new LinkedList<ReplicatedRow>();
		HashMap<Integer, Byte> usedCols= new LinkedHashMap<Integer, Byte>();
		HashMap<Integer, Byte> usedUpdateCols= new LinkedHashMap<Integer, Byte>();

        BitSet usedBits = msg.colPresentBitmap;
        BitSet usedUpdateBits = msg.colPresentBitmapAftImg;
        
        for (int i=0; i<map.colCount;i++) {
        	if (usedBits.get(i))
        	{
	            usedCols.put(i,map.colTypeDef[i]);
        	}
	        if (msg.eventType==ReplicationPacket.UPDATE_ROWS_EVENT)
	        {
	        	if (usedUpdateBits.get(i))
	        	{
	        		usedUpdateCols.put(i,map.colTypeDef[i]);
	        	}
	        }
        }
        
        while (msg.rawRows.isReadable())
        {
            int presentCount = (usedCols.size() + 7) / 8;
            ReplicatedRow row = new ReplicatedRow();
            byte[] nullBitmap = new byte[presentCount];
            msg.rawRows.readBytes(nullBitmap);
            BitSet nullBits = BitSet.valueOf(nullBitmap);
	
            int i =0;
	        for (Integer key: usedCols.keySet()) {
	        	BindValue value = new BindValue();
	        	value.type = usedCols.get(key);
 	            value.isNull =nullBits.get(i);
	            if (!value.isNull) {
                    BindValueUtil.read(msg.rawRows, value);
	            }
	            row.colValue.put(key, value);
	            i++;
	        }
	        
	        if (msg.eventType==ReplicationPacket.UPDATE_ROWS_EVENT)
	        {
	            int presentUpdateCount = (usedUpdateCols.size() + 7) / 8;
	            byte[] nullBitmapAftImg = new byte[presentUpdateCount];
	            msg.rawRows.readBytes(nullBitmapAftImg);
	            BitSet nullBitsAfter = BitSet.valueOf(nullBitmapAftImg);
		
	            int j =0;
		        for (Integer key: usedUpdateCols.keySet()) {
		        	BindValue value = new BindValue();
		        	value.type = usedUpdateCols.get(key);
	 	            value.isNull =nullBitsAfter.get(j);
		            if (!value.isNull) {
	                    BindValueUtil.read(msg.rawRows, value);
		            }
		            row.colValueAftImg.put(key, value);
		            j++;
		        }
	        	
	        }
	        
	        rows.add(row);
        
	        _log.trace("table name: {}.{}", map.schemaName, map.tableName);
	        _log.trace("event type: {}", msg.eventType);
	        _log.trace("row: {}", row.colValue);
	        if (msg.eventType==ReplicationPacket.UPDATE_ROWS_EVENT)
		        _log.trace("rowAftImg: {}", row.colValueAftImg);
        }

	}
	
    protected int getCapabilities() {
        int flag = 0;
        flag |= CLIENT_CONNECT_WITH_DB;
        flag |= CLIENT_PROTOCOL_41;
        flag |= CLIENT_TRANSACTIONS;
        flag |= CLIENT_RESERVED;
        flag |= CLIENT_PLUGIN_AUTH;
        flag |= CLIENT_SECURE_CONNECTION;
        if (enableSSL())
        	flag |= CLIENT_SSL;
        return flag;
    }

	private boolean enableSSL()
	{
    	String keyFile = fish.getConfig().getSSLKeyFile();
    	String password = fish.getConfig().getSSLPassword();
    	if (keyFile!=null || password!=null)
    	{
    		return true;
    	}
    	return false;
	}
	
    public void state(ChannelHandlerContext ctx, StateIndicator msg)
	{
        ByteBuf buf = ctx.alloc().buffer();
    	if (msg.eventType == StateIndicator.INITIAL_STATE)
    	{
            PacketEncoder.writePacket(buf, (byte)1, () -> packetEncoder.writeHandshakeResponse(
            		buf,
            		getCapabilities(),
            		masterUser, 
            		masterPassword));
            ctx.writeAndFlush(buf);
    	}
    	else if (msg.eventType == StateIndicator.HANDSHAKEN_STATE)
    	{
            PacketEncoder.writePacket(buf, (byte)0, () -> packetEncoder.writeRegisterSlave(
            		buf,
            		getConfig().getSlaveServerId()));
            ctx.writeAndFlush(buf);
            _log.info("Replication handshaken" );
    	}
    	else if (msg.eventType == StateIndicator.REGISTERED_STATE)
    	{
            this.session = this.fish.getOrca().createSession(masterUser, getClass().getSimpleName());
            PacketEncoder.writePacket(buf, (byte)0, () -> packetEncoder.writeBinlogDump(
            		buf,
            		masterLogPos,
            		getConfig().getSlaveServerId(), 
            		masterBinlog));
            ctx.writeAndFlush(buf);
            _log.info("Slave registered" );
    	}
    	else if (msg.eventType == StateIndicator.HANDSHAKE_FAIL_STATE ||
    			msg.eventType == StateIndicator.REGISTER_FAIL_STATE)
    	{
            _log.error("Replication failed to start" );
    		close(ctx);
    	}
    	
	}
	
	private ConfigService getConfig() {
		return this.fish.getOrca().getConfigService();
	}

	public void close(ChannelHandlerContext ctx) {
        ctx.channel().close();
	}
	
	CheckPoint getCheckPoint() {
		return this.fish.getOrca().getHumpback().getCheckPoint();
	}
}
