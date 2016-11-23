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
package com.antsdb.saltedfish.server.mysql.packet;

import java.util.HashMap;
import java.util.Map;

/**
 * defines the different types of mysql packets
 * 
 * id between 0-255 is standard mysql packet type. beyond 255 is used to identify packets that can't be identified 
 * using COMMAND byte
 * 
 * @author wgu0
 */
public enum PacketType {
    /**
     * none, this is an internal thread state
     */
    COM_SLEEP(0),
    /**
     * mysql_close
     */
    COM_QUIT(1),
    /**
     * mysql_select_db
     */
    COM_INIT_DB(2),
    /**
     * mysql_real_query
     */
    COM_QUERY(3),
    /**
     * mysql_list_fields
     */
    COM_FIELD_LIST(4),
    /**
     * mysql_create_db (deprecated)
     */
    COM_CREATE_DB(5),
    /**
     * mysql_drop_db (deprecated)
     */
    COM_DROP_DB(6),
    /**
     * mysql_refresh
     */
    COM_REFRESH(7),
    /**
     * mysql_shutdown
     */
    COM_SHUTDOWN(8),
    /**
     * mysql_stat
     */
    COM_STATISTICS(9),
    /**
     * mysql_list_processes
     */
    COM_PROCESS_INFO(10),
    /**
     * none, this is an internal thread state
     */
    COM_CONNECT(11),
    /**
     * mysql_kill
     */
    COM_PROCESS_KILL(12),
    /**
     * mysql_dump_debug_info
     */
    COM_DEBUG(13),
    /**
     * mysql_ping
     */
    COM_PING(14),
    /**
     * none, this is an internal thread state
     */
    COM_TIME(15),
    /**
     * none, this is an internal thread state
     */
    COM_DELAYED_INSERT(16),
    /**
     * mysql_change_user
     */
    COM_CHANGE_USER(17),
    /**
     * used by slave server mysqlbinlog
     */
    COM_BINLOG_DUMP(18),
    /**
     * used by slave server to get master table
     */
    COM_TABLE_DUMP(19),
    /**
     * used by slave to log connection to master
     */
    COM_CONNECT_OUT(20),
    /**
     * used by slave to register to master
     */
    COM_REGISTER_SLAVE(21),
    /**
     * mysql_stmt_prepare
     */
    COM_STMT_PREPARE(22),
    /**
     * mysql_stmt_execute
     */
    COM_STMT_EXECUTE(23),
    /**
     * mysql_stmt_send_long_data
     */
    COM_STMT_SEND_LONG_DATA(24),
    /**
     * mysql_stmt_close
     */
    COM_STMT_CLOSE(25),
    /**
     * mysql_stmt_reset
     */
    COM_STMT_RESET(26),
    /**
     * mysql_set_server_option
     */
    COM_SET_OPTION(27),
    /**
     * mysql_stmt_fetch
     */
    COM_STMT_FETCH(28),
    /**
     * roger heartbeat
     */
    COM_HEARTBEAT(64),
    /** starting from here are not standard mysql type */
    FISH_HANDSHAKE(0x100), 
    FISH_AUTH(0x101),
    FISH_ERROR(0x102),
    FISH_EOF(0x103),
    FISH_AUTH_OK(0x104),
    FISH_OK(0x105),
    FISH_RESULT_SET_HEADER(0x110),
    FISH_RESULT_SET_COLUMN(0x111),
    FISH_RESULT_SET_ROW(0x112),
    /**/
    VERY_LARGE_PACKET(0x10000);
    ;

	static Map<Integer, PacketType> _typeById = new HashMap<>();
    
	private final int id;
	
	static {
		for (PacketType i:PacketType.values()) {
			_typeById.put(i.getId(), i);
		}
	}
	
	PacketType(int id) {
		this.id = id;
	}
	
	public int getId() {
		return this.id;
	}
	
	public static PacketType valueOf(int id) {
		return _typeById.get(id);
	}
}
