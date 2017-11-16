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

import java.util.List;

import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.sql.planner.SortKey;
import com.antsdb.saltedfish.sql.vdm.Checks;
import com.antsdb.saltedfish.sql.vdm.Cursor;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.Script;
import com.antsdb.saltedfish.sql.vdm.VdmContext;

public class ShowTables extends CursorMaker {
	String namespace;
	boolean isFull;
	String sql;
	Script upstream;
	String like;
	
	public ShowTables(Session session, String namespace, boolean isFull, String like) {
		super();
		this.namespace = namespace;
		this.isFull = isFull;
		this.like = like;
        if (this.isFull) {
            sql = "SELECT table_name, 'BASE TABLE' as Table_type FROM " + Orca.SYSNS + ".systable WHERE namespace=?";
        }
        else {
            sql = "SELECT table_name FROM " + Orca.SYSNS + ".systable WHERE namespace=?";
        }
        if (like != null) {
        	sql += " AND table_name LIKE ?";
        }
		this.upstream = session.parse(sql);
	}
	
    public void setLike(String like) {
		this.like = like;
	}

	@Override
    public CursorMeta getCursorMeta() {
        return upstream.getCursorMeta();
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters notused, long notused1) {

    	// get current namespace if not specified
    	
    	String ns = this.namespace;
    	if (ns == null) {
    		ns = ctx.getSession().getCurrentNamespace();
    	}
        if (ns == null) {
            throw new OrcaException("namespace is not specified");
        }
    	
        // normalize the namespace with real name
    	
        ns = Checks.namespaceExist(ctx.getOrca(), ns);
        
        // like
        
        Parameters params;
        if (this.like != null) {
        	params = new Parameters(new Object[] {ns, this.like});
        }
        else {
        	params = new Parameters(new Object[] {ns});
        }
        
        // run it

        Cursor c = (Cursor)this.upstream.run(ctx, params, 0);
        
        return c;
    }

    @Override
    public boolean setSortingOrder(List<SortKey> order) {
        return false;
    }
}
