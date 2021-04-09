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
package com.antsdb.saltedfish.parquet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class ParquetFilter implements PathFilter {

    private String prefix;

    private String suffix;

    public ParquetFilter(String prefix, String suffix) {
        this.prefix = prefix;
        this.suffix = suffix;
    }

    @Override
    public boolean accept(Path p) {
        boolean flag = !p.getName().startsWith(".");

        if (prefix != null && prefix.length() > 0) {
            flag = p.getName().startsWith(prefix) ;
        }
        if (flag && suffix != null && suffix.length() > 0) {
            flag = p.getName().endsWith(suffix);
        }
        return flag;
    }

}
