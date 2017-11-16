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

import java.util.List;

import com.antsdb.saltedfish.lexer.MysqlParser.Limit_clauseContext;
import com.antsdb.saltedfish.sql.planner.SortKey;

public abstract class CursorMaker extends Instruction {
    int makerId;
    
    public abstract CursorMeta getCursorMeta();
    public abstract boolean setSortingOrder(List<SortKey> order);
    
    public Cursor make(VdmContext ctx, Parameters params, long pMaster) {
        return (Cursor)run(ctx, params, pMaster);
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        return null;
    }

    @Override
    public void explain(int level, List<ExplainRecord> records) {
        ExplainRecord rec = new ExplainRecord(level, toString());
        rec.setMakerId(makerId);
        records.add(rec);
    }
    
    @Override
	public String toString() {
		return getClass().getSimpleName();
	}

	public int getMakerid() {
    	return this.makerId;
    }
    
    public void setMakerId(int value) {
    	this.makerId = value;
    }
    
    public static CursorMaker createLimiter(CursorMaker maker, Limit_clauseContext rule) {
    	int offset = 0;
    	int count = Integer.parseInt(rule.number_value(0).getText());
		if (rule.K_OFFSET() != null) {
    		offset = Integer.parseInt(rule.number_value(1).getText());
		}
		else {
	    	if (rule.number_value(1) != null) {
	    		offset = count;
	    		count = Integer.parseInt(rule.number_value(1).getText());
	    	}
		}
		maker = new Limiter(maker, offset, count);
		return maker;
	}

}
