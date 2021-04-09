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
import com.antsdb.saltedfish.cpp.RecyclableHeap;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.HumpbackSession;
import com.antsdb.saltedfish.nosql.SpaceManager;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.sql.SystemParameters;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.TableMeta;

public final class VdmContext {
    Session session;
    long readTrxTs;
    long writeTrx;
    Object[] variables;
    private Transaction trx;
    List<AtomicLong> cursorStats;
    private VdmComparator comp;
    private long pGroup;
    private Profiler profiler;
    private RecyclableHeap gheap;

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
    
    /**
     * 
     * @param heap
     * @param params
     * @param pRecord
     * @param x
     * @param y
     * @return Integer.MIN_VALUE if one of the two values is null
     */
    public int compare(Heap heap, Parameters params, long pRecord, Operator x, Operator y) {
        if (this.comp == null) {
            this.comp = new VdmComparator(this);
        }
        return this.comp.comp(heap, params, pRecord, x, y); 
    }

    /**
     * 
     * @param heap
     * @param pRecord
     * @param px
     * @param py
     * @return Integer.MIN_VALUE if one of the two values is null
     */
    public int compare(Heap heap, long px, long py) {
        if (this.comp == null) {
            this.comp = new VdmComparator(this);
        }
        return this.comp.comp(heap, px, py); 
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

    public SystemParameters getConfig() {
        return this.session.getConfig();
    }
    
    public int getvariableCount() {
        return this.variables.length;
    }

    public void setGroupContext(long pGroup) {
        this.pGroup = pGroup;
    }
    
    public long getGroupContext() {
        return this.pGroup;
    }
    
    public long getGroupVariable(int variableId) {
        long pGroup = getGroupContext();
        GroupContext group = pGroup != 0 ? new GroupContext(pGroup) : null;
        return group != null ? group.getVarialbe(variableId) : 0;
    }

    public void setGroupVariable(int variableId, long value) {
        long pGroup = getGroupContext();
        if (pGroup == 0) {
            throw new IllegalArgumentException();
        }
        GroupContext group = new GroupContext(pGroup);
        group.setVariable(variableId, value);
    }

    public void setProfiler(Profiler profiler) {
        this.profiler = profiler;
    }
    
    public CursorStats getProfiler(int makerId) {
        return this.profiler != null ? this.profiler.getStats(makerId) : null;
    }
    
    public RecyclableHeap getGroupHeap() {
        return this.gheap;
    }
    
    public void setGroupHeap(RecyclableHeap gheap) {
        this.gheap = gheap;
    }
}
