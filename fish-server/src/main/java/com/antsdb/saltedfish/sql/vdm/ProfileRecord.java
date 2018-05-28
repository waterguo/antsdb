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

class ProfileRecord extends HashMapRecord {
    ProfileRecord() {
    }
    
    ProfileRecord(int level, String plan, String stats) {
        this.setLevel(level)
            .setPlan(plan)
            .setStats(stats);
    }
    
    int getMakerId() {
    	return (int)this.get(0);
    }
    
    ProfileRecord setMakerId(int value) {
    	this.set(0, value);
    	return this;
    }
    
    int getLevel() {
        return (int)this.get(1);
    }
    
    ProfileRecord setLevel(int level) {
        this.set(1, level);
        return this;
    }
    
    String getPlan() {
        return (String)this.get(2);
    }
    
    ProfileRecord setPlan(String plan) {
        this.set(2, plan);
        return this;
    }
    
    String getStats() {
        return (String)this.get(3);
    }
    
    ProfileRecord setStats(String stats) {
        this.set(3, stats);
        return this;
    }
}
