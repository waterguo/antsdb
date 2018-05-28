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

import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.commons.codec.Charsets;
import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;
import com.mysql.jdbc.Security;

/**
 * 
 * @author *-xguo0<@
 */
public class NativeAuthPlugin extends AuthPlugin {
    final static Logger _log = UberUtil.getThisLogger();
    
    private byte[] seed;
    public NativeAuthPlugin(Orca orca) {
        super(orca);
        this.seed = orca.getConfig().getSeed();
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
        try {
            return Arrays.equals(password, getHash(user));
        }
        catch (Exception x) {
            _log.error("error", x);
        }
        return false;
    }

    @Override
    public byte[] hash(String password) {
        try {
            byte[] hash;
            hash = Security.scramble411(password, new String(seed, Charsets.UTF_8), Charset.defaultCharset().name());
            return hash;
        }
        catch (Exception x) {
            throw new OrcaException(x, "unable to hash");
        }
    }
}
