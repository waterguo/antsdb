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
package com.antsdb.saltedfish.sql;

import org.apache.commons.codec.Charsets;

/**
 * 
 * @author *-xguo0<@
 */
public class NoPasswordAuthPlugin extends AuthPlugin {

    byte[] seed  = new byte[] {
            0x50, 0x3a, 0x6e, 0x3d, 0x25, 0x40, 0x51, 0x56, 0x73, 0x68, 
            0x2f, 0x50, 0x27, 0x6f, 0x7a, 0x38, 0x46, 0x38, 0x26, 0x51};

    NoPasswordAuthPlugin(Orca orca) {
        super(orca);
    }

    @Override
    public String getName() {
        return "mysql_native_password";
    }
    
    @Override
    public byte[] getSeed() {
        return this.seed;
    }

    @Override
    public boolean authenticate(String user, byte[] password) {
        return true;
    }

    @Override
    public byte[] hash(String password) {
        return password.getBytes(Charsets.UTF_8);
    }
}
