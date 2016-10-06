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
package com.antsdb.saltedfish.sql.mysql;

import java.util.Collections;

import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class USER extends CursorMaker {

	Orca orca;
    CursorMeta meta;
    
    public static class Item {
    	public String Host;
    	public String User;
    	public String Password;
    	public String Select_priv;
    	public String Insert_priv;
    	public String Update_priv;
    	public String Delete_priv;
    	public String Create_priv;
    	public String Drop_priv;
    	public String Reload_priv;
    	public String Shutdown_priv;
    	public String Process_priv;
    	public String File_priv;
    	public String Grant_priv;
    	public String References_priv;
    	public String Index_priv;
    	public String Alter_priv;
    	public String Show_db_priv;
    	public String Super_priv;
    	public String Create_tmp_table_priv;
    	public String Lock_tables_priv;
    	public String Execute_priv;
    	public String Repl_slave_priv;
    	public String Repl_client_priv;
    	public String Create_view_priv;
    	public String Show_view_priv;
    	public String Create_routine_priv;
    	public String Alter_routine_priv;
    	public String Create_user_priv;
    	public String Event_priv;
    	public String Trigger_priv;
    	public String Create_tablespace_priv;
    	public String ssl_type;
    	public byte[] ssl_cipher;
    	public byte[] x509_issuer;
    	public byte[] x509_subject;
    	public Integer max_questions;
    	public Integer max_updates;
    	public Integer max_connections;
    	public Integer max_user_connections;
    	public String plugin;
    	public String authentication_string;
    }
    
    public USER(Orca orca) {
        this.orca = orca;
    	this.meta = CursorUtil.toMeta(Item.class); 
    }

	@Override
	public CursorMeta getCursorMeta() {
		return this.meta;
	}
    
    @Override
	public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Cursor c = CursorUtil.toCursor(meta, Collections.emptyList());
        return c;
    }
}
