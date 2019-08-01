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
package com.antsdb.saltedfish.sql.vdm;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.HumpbackSession;
import com.antsdb.saltedfish.nosql.SpaceManager;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.TableMeta;

public class VdmContext {
    Session session;
    long readTrxTs;
    long writeTrx;
    Object[] variables;
    private Transaction trx;
    List<AtomicLong> cursorStats;
    private VdmComparator comp;

    public VdmContext(Session session, int nVariables) {
        super();
        this.session = session;
        this.variables = new Object[nVariables];
    }
    
    public Orca getOrca() {
        return this.session.getOrca();
    }

    public Session getSession() {
        return session;
    }
    
    /**
     * clone and freeze the current transaction context
     * @return
     */
    public VdmContext freeze() {
        VdmContext newone = new VdmContext(session, this.variables.length);
        this.trx = getTransaction();
        return newone;
    }
    
    public Transaction getTransaction() {
        if (this.trx == null) {
            return this.session.getTransaction();
        }
        else {
            // context is frozen. return frozen transaction instead of the one from session 
            return this.trx;
        }
    }
    
    public Humpback getHumpback() {
        return getOrca().getHumpback();
    }

    public MetadataService getMetaService() {
        return getOrca().getMetaService();
    }
    
    public GTable getGtable(ObjectName name) {
        TableMeta tableMeta = getMetaService().getTable(getTransaction(), name);
        GTable table = this.getHumpback().getTable(tableMeta.getHtableId());
        return table;
    }

    public Object getVariable(int variableId) {
        return this.variables[variableId];
    }
    
    public void setVariable(int variableId, Object variable) {
        this.variables[variableId] = variable;
    }

    public final SpaceManager getSpaceManager() {
        return getOrca().getHumpback().getSpaceManager();
    }

    public AtomicLong getCursorStats(int makerId) {
        if (this.cursorStats == null) {
            this.cursorStats = new ArrayList<>();
        }
        while (this.cursorStats.size() <= (makerId+1)) {
            this.cursorStats.add(new AtomicLong());
        }
        return this.cursorStats.get(makerId);
    }
    
    public Integer compare(Heap heap, Parameters params, long pRecord, Operator x, Operator y) {
        if (this.comp == null) {
            this.comp = new VdmComparator(this);
        }
        return this.comp.comp(heap, params, pRecord, x, y); 
    }

    public HumpbackSession getHSession() {
        return this.session.getHSession();
    }
    
    /**
     * perform a conversion which is affect by mysql strict sql mode
     * 
     * @see https://dev.mysql.com/doc/refman/5.7/en/sql-mode.html#sql-mode-strict
     * @param call a conversion function
     * @return null if conversion failed and strict is on
     */
    <T> T strict(Supplier<T> call) {
        if (this.session.getConfig().isStrict()) {
            return call.get();
        }
        else {
            try {
                return call.get();
            }
            catch (Exception x) {
                return null;
            }
        }
    }
}
