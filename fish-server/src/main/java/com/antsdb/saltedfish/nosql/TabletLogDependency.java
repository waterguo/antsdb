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
package com.antsdb.saltedfish.nosql;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author *-xguo0<@
 */
public class TabletLogDependency implements LogDependency {
    private Humpback humpback;

    private class TableDependency implements LogDependency {

        public String name;
        public GTable gtable;

        @Override
        public String getName() {
            return this.name;
        }

        @Override
        public List<LogDependency> getChildren() {
            List<LogDependency> result = new ArrayList<>();
            for (MemTablet i:this.gtable.getTablets()) {
                result.add(i);
            }
            return result;
        }
        
    }
    private class NameSpaceDependency implements LogDependency {
        public String name;
        
        @Override
        public String getName() {
            return this.name;
        }

        @Override
        public List<LogDependency> getChildren() {
            Humpback humpback = TabletLogDependency.this.humpback;
            List<LogDependency> result = new ArrayList<>();
            for (GTable i:humpback.getTables(this.name)) {
                TableDependency ii = new TableDependency();
                ii.name = humpback.getTableInfo(i.getId()).getTableName();
                ii.gtable = i;
                if (ii.getChildren().size() > 0) {
                    result.add(ii);
                }
            }
            return result;
        }
    }
    
    TabletLogDependency(Humpback humpback) {
        this.humpback = humpback;
    }
    
    @Override
    public String getName() {
        return "tablets";
    }

    @Override
    public List<LogDependency> getChildren() {
        List<LogDependency> result = new ArrayList<>();
        for (String i:this.humpback.getNamespaces()) {
            NameSpaceDependency ii = new NameSpaceDependency();
            ii.name = i;
            result.add(ii);
        }
        return result;
    }

}
