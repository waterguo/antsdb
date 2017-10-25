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

import java.io.Closeable;

import com.antsdb.saltedfish.cpp.FlexibleHeap;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.nosql.VaporizingRow;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.PreparedStatement;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.sql.vdm.FishParameters;
import com.antsdb.saltedfish.sql.vdm.Parameters;

import io.netty.buffer.ByteBuf;

/**
 * @author roger
 */
public class MysqlPreparedStatement implements Closeable {

    public int[] types;
    PreparedStatement script;
    Heap heap = new FlexibleHeap();
    VaporizingRow row;
    
    public MysqlPreparedStatement(Orca orca, PreparedStatement script) {
        super();
        this.script = script;
        this.row = new VaporizingRow(heap, script.getParameterCount()-1);
    }

    public int getId() {
        return script.hashCode();
    }

    public int getParameterCount() {
        return this.script.getParameterCount();
    }

	public Heap getHeap() {
		return this.heap;
	}

	public void setParam(int paramId, long pValue) {
    	row.setFieldAddress(paramId, pValue);
	}

	public Parameters getParams() {
		return new FishParameters(this.row);
	}

	public Object run(Session session) {
		FishParameters params = new FishParameters(row);
		Object result = this.script.run(session, params);
		return result;
	}

	@Override
	public void close() {
		if (this.heap != null) {
			this.heap.free();
		}
		this.row = null;
		this.heap = null;
	}

	public void clear() {
		this.heap.reset(0);
		this.row = new VaporizingRow(heap, getParameterCount()-1);
	}

	public void setParam(int paramId, ByteBuf content) {
	}

	public ByteBuf getLongData(int i) {
		return null;
	}
	
	public String getSql() {
		return this.script.getSql();
	}
	
	public VaporizingRow getParameters() {
	    return this.row;
	}
}