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
package com.antsdb.saltedfish.sql.planner;

import java.util.ArrayList;
import java.util.List;

import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.Operator;

/**
 * Node is a participant in a query that can product a cursor
 *  
 * @author wgu0
 */
class Node {
    TableMeta table;
    CursorMaker maker;
    private List<ColumnFilter> filters = new ArrayList<ColumnFilter>();
    ObjectName alias;
    List<PlannerField> fields = new ArrayList<>();
    boolean isOuter;
    boolean isParent = false;
    Operator joinCondition;
    
    // below are fields related to union
    
    List<Node> unions;
	Node active;
	Operator where;
    
    PlannerField findField(FieldMeta field) {
        for (PlannerField i:fields) {
            if (i == field) {
                return i;
            }
        }
        return null;
    }

    int findFieldPos(PlannerField field) {
        for (int i=0; i<this.fields.size(); i++) {
        	if (this.fields.get(i) == field) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public String toString() {
        return this.alias.toString();
    }

	public void setActive(Node child) {
		this.active = child;
	}

	public void addFilter(ColumnFilter cf) {
		if (this.active != null) {
			this.active.addFilter(cf);
		}
		else {
			this.filters.add(cf);
		}
	}
	
	public List<ColumnFilter> getFilters() {
		// dont change the stupid logic below. see Analyzer.analyze()
		if (this.isUnion()) {
			return this.filters;
		}
		else {
			return this.filters;
		}
	}

	public boolean isUnion() {
		return this.unions != null;
	}
}
