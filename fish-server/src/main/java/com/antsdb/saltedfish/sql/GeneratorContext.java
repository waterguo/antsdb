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
package com.antsdb.saltedfish.sql;

import java.util.IdentityHashMap;
import java.util.Map;

import org.antlr.v4.runtime.tree.TerminalNode;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.Transaction;


public class GeneratorContext {
    Session session;
    Map<TerminalNode, Integer> parameterByNode = new IdentityHashMap<>();
    boolean hasAggregateFunctions = false;
    int variablesCount;
    boolean compileDdl = false;
    int nextMakerId = 1;
    
    public Humpback getHumpback() {
        return getOrca().getHumpback();
    }
    
    public boolean isCompileDdl() {
        return compileDdl;
    }

    public void setCompileDdl(boolean compileDdl) {
        this.compileDdl = compileDdl;
    }

    public GeneratorContext(Session session) {
        super();
        this.session = session;
    }

    public Session getSession() {
        return session;
    }
    
    public Orca getOrca() {
        return session.getOrca();
    }
    
    public GTable getGtable(ObjectName name) {
        TableMeta tableMeta = getOrca().getMetaService().getTable(session.getTransaction(), name);
        return getOrca().getHumpback().getTable(name.getNamespace(), tableMeta.getHtableId());
    }
    
    public void addParameter(TerminalNode node) {
        this.parameterByNode.put(node, this.parameterByNode.size());
    }
    
    public int getParameterCount() {
        return this.parameterByNode.size();
    }

    public int getParameterPosition(TerminalNode node) {
        Integer pos = this.parameterByNode.get(node);
        return pos;
    }
    
    public TableMeta getTable(ObjectName name) {
        MetadataService metaService = getOrca().getMetaService();
        Transaction trx = getSession().getTransaction();
        TableMeta table = metaService.getTable(trx, name);
        return table;
    }

	public int allocVariable() {
		return this.variablesCount++;
	}
	
	public int getVariableCount() {
		return this.variablesCount;
	}

	public DataTypeFactory getTypeFactory() {
		return getOrca().getTypeFactory();
	}
	
	/**
	 * maker id is used to trace performance stats
	 * 
	 * @return
	 */
	public int getNextMakerId() {
		return this.nextMakerId++;
	}
}
