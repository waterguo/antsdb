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

import com.antsdb.saltedfish.nosql.Statistician;

/**
 * 
 * @author *-xguo0<@
 */
public class PathUnion extends Path {
    List<Path> paths = new ArrayList<>();
    
    PathUnion() {
        super(null);
    }

    @Override
    double getScore(Statistician stats) {
        double result = 0;
        for (Path i:this.paths) {
            result += i.getScore(stats);
        }
        return result;
    }

    void add(Path path) {
        this.paths.add(path);
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        for (Path i:this.paths) {
            if (buf.length() != 0) {
                buf.append(" + ");
            }
            String s = i.toString();
            buf.append('(');
            buf.append(s);
            buf.append(')');
        }
        return buf.toString();
    }
    
    
}
