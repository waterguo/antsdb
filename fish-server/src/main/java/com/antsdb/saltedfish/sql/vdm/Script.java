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
package com.antsdb.saltedfish.sql.vdm;

import org.slf4j.Logger;

import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.util.UberUtil;

public class Script extends Instruction {
    static Logger _log = UberUtil.getThisLogger();
    
    Instruction root;
    int nParameters;
    int nVariables;
    String sql;
    Measure measure = new Measure();
    
    public Script(Instruction root, int nParameters, int nVariables, String sql) {
        super();
        this.root = root;
        this.nParameters = nParameters;
        this.sql = sql;
        this.nVariables = nVariables;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        long start = 0;
        if (_log.isTraceEnabled()) {
            _log.trace("run {}-{}: {}", ctx.getSession().getId(), hashCode(), params.toString());
            start = System.currentTimeMillis();
        }
        if (params.size() < this.nParameters) {
            throw new OrcaException("insufficient parameters specified for prepared statement");
        }
        Object result;
        if (ctx.session.isAutoCommit()) {
            result = run_auto_commit(ctx, params);
        }
        else {
            result = run_(ctx, params);
        }
        if (_log.isTraceEnabled()) {
            long end = System.currentTimeMillis();
            long elapse = end - start;
            _log.trace("elapsed time for {}: {}", hashCode(), elapse);
        }
        return result;
    }

    private Object run_auto_commit(VdmContext ctx, Parameters params) {
        boolean success = false;
        try {
            Object result = run_(ctx, params);
            success = true;
            return result;
        }
        finally {
            if (success) {
                ctx.session.commit();
            }
            else {
                ctx.session.rollback();
            }
        }
    }

    private Object run_(VdmContext ctx, Parameters params) {
        long start = 0;
        Measure m = this.measure;
        start = m.getTime();
        try {
            ctx.session.startTrx();
            // read commited isolation level. reset visibility every statement execution
            ctx.session.resetTrxTs();
            Object result;
            if (this.root != null) {
            	result = this.root.run(ctx, params, 0);
            }
            else {
            	// sql has ddl. run in interpretation mode
            	result = ctx.getSession().getParserFactory().run(ctx, params, this.sql);
            }
            return result;
        }
        finally {
            if (m != null) {
                m.measure(start);
            }
        }
    }

    public int getParameterCount() {
        return this.nParameters;
    }
    
    public int getVariableCount() {
        return this.nVariables;
    }
    
    public String getSql() {
        return sql;
    }
    
    public CursorMeta getCursorMeta() {
        if (root instanceof CursorMaker) {
            return ((CursorMaker)root).getCursorMeta();
        }
        return null;
    }

    /**
     * at this point. only cache the simple stuff. 
     * 
     * because
     * 
     * - ddl cannot be cached
     * - large volume of unprepared statements might compromise performance such as initial loading inserts
     * 
     * @return
     */
    public boolean worthCache() {
        if (this.nParameters > 0) {
            return true;
        }
        return worthCache(this.root);
    }

    private boolean worthCache(Instruction step) {
        if (step instanceof Commit) {
            return true;
        }
        else if (step instanceof Rollback) {
            return true;
        }
        else if (step instanceof SetSystemParameter) {
            return true;
        }
        else if (step instanceof StatementWrapper) {
            return worthCache(((StatementWrapper)step).stmt);
        }
        else if (step instanceof Flow) {
            Flow flow = (Flow)step;
            if (flow.instructions.size() == 1) {
                return worthCache(flow.instructions.get(0));
            }
        }
        return false;
    }

    public Measure getMeasure() {
        return this.measure;
    }
    
    public Instruction getRoot() {
        return this.root;
    }
}
