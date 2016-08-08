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

class ExplainRecord extends HashMapRecord {
    ExplainRecord() {
    }
    
    ExplainRecord(int level, String plan) {
        this.setLevel(level)
            .setPlan(plan)
            .setMakerId(0);
    }
    
    int getLevel() {
        return (int)this.get(0);
    }
    
    ExplainRecord setLevel(int level) {
        this.set(0, level);
        return this;
    }
    
    String getPlan() {
        return (String)this.get(1);
    }
    
    ExplainRecord setPlan(String plan) {
        this.set(1, plan);
        return this;
    }
    
    int getMakerId() {
    	return (int)this.get(2);
    }
    
    ExplainRecord setMakerId(int value) {
    	this.set(2, value);
    	return this;
    }
}
