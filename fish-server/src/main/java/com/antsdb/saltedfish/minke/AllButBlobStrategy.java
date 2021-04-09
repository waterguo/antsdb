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
package com.antsdb.saltedfish.minke;

import com.antsdb.saltedfish.nosql.SysMetaRow;

/**
 * fully cache everything except blob tables
 * 
 * @author *-xguo0<@
 */
public class AllButBlobStrategy extends CacheStrategy {

    @Override
    boolean cacheFull(SysMetaRow meta) {
        return !meta.getTableName().endsWith("_blob_");
    }

}
