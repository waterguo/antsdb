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
 * define caching behavior. 
 * 
 * @author *-xguo0<@
 */
public abstract class CacheStrategy {
    /**
     * decides the cache strategy at table level. fully cached table will have its entire content sitting
     * in minke. partially cached table will have its content cached depends on access pattern. partially 
     * cached table can be very slow in some cases 
     * 
     * @param tableId
     * @param meta not null
     * @return
     */
    abstract boolean cacheFull(SysMetaRow meta);
}
