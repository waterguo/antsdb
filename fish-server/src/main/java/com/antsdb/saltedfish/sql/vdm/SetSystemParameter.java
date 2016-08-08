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

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;

import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.util.UberUtil;

public class SetSystemParameter extends Statement {
    static Logger _log = UberUtil.getThisLogger();
    
    public enum Scope {
        USER,
        SESSION,
        GLOBAL,
    }
    
    Scope scope;
    String name;
    Operator expr;
    String constant;
    
    // set to default
    public SetSystemParameter(Scope scope, String name) {
        this.scope = scope;
        this.name = name;
    }
    
    public SetSystemParameter(Scope scope, String name, Operator expr) {
        super();
        this.name = name;
        this.scope = scope;
        this.expr = expr;
    }

    public SetSystemParameter(Scope scope, String name, String value) {
        super();
        this.name = name;
        this.scope = scope;
        this.constant = value;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        Object value = null;
        if (this.expr != null) {
            // calculate the value
            value = Util.eval(ctx, this.expr, params, 0);
        }
        if (this.name.equalsIgnoreCase("autocommit")) {
            if (!(value instanceof Number)) {
                throw new OrcaException("autocommit must be 1 or 0");
            }
            Integer autoCommit = ((Number)value).intValue();
            if ((autoCommit > 1) || (autoCommit < 0)) {
                throw new OrcaException("autocommit must be 1 or 0");
            }
            ctx.getSession().setAutoCommit(autoCommit == 1);
        }
        else if (this.name.equals("character_set_results")) {
        	if (!"utf8".equals(this.constant)) {
        		_log.warn("setting character_set_results to {} is ignored", this.constant);
        	}
        }
        else {
        	ctx.getSession().setParameter(name, value);
        }
        return null;
    }

    @Override
    List<TableMeta> getDependents() {
        return Collections.emptyList();
    }

}
