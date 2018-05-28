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
package com.antsdb.saltedfish.sql;

import com.antsdb.saltedfish.sql.meta.UserMeta;

/**
 * 
 * @author *-xguo0<@
 */
public abstract class AuthPlugin {
    Orca orca;
    
    abstract public String getName();
    abstract public byte[] getSeed();
    abstract public boolean authenticate(String user, byte[] password);
    abstract public byte[] hash(String password);
    
    AuthPlugin(Orca orca) {
        this.orca = orca;
    }
    
    byte[] getHash(String user) {
        UserMeta userMeta = this.orca.getMetaService().getUser(user);
        return (userMeta != null) ? userMeta.getPassword() : null;
    }
}
