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

import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.util.UberUtil;

public class SetSystemParameter extends Statement {
    static Logger _log = UberUtil.getThisLogger();
    
    Scope scope;
    String name;
    Operator expr;
    String constant;

    private boolean permanent;
    
    public enum Scope {
        SESSION,
        GLOBAL,
    }
    
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

    public void setPermanent(boolean value) {
        this.permanent = value;
    }
    
    @Override
    public Object run(VdmContext ctx, Parameters params) {
        Object value = null;
        if (this.expr != null) {
            // calculate the value
            value = Util.eval(ctx, this.expr, params, 0);
        }
        else {
        	    value = this.constant;
        }
        if (this.scope == Scope.GLOBAL) {
            if (this.permanent) {
                ctx.getOrca().getConfig().setPermanent(name, value == null ? null : value.toString());
            }
            else {
                ctx.getOrca().getConfig().set(name, value == null ? null : value.toString());
            }
        }
        else {
            ctx.getSession().getConfig().set(name, value == null ? null : value.toString());
        }
        return null;
    }

    @Override
    List<TableMeta> getDependents() {
        return Collections.emptyList();
    }

}
