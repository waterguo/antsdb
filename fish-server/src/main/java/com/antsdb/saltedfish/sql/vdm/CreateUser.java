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

import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.UserMeta;

/**
 * 
 * @author *-xguo0<@
 */
public class CreateUser extends Instruction {

    private String user;
    private String password;
    private boolean ifNotExists;

    public CreateUser(String user, String password, boolean ifNotExists) {
        this.user = user;
        this.password = password;
        this.ifNotExists = ifNotExists;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        MetadataService meta = ctx.getOrca().getMetaService();
        UserMeta userMeta = meta.getUser(this.user);
        if (userMeta != null) {
            if (ifNotExists) {
                return null;
            }
            else {
                throw new OrcaException("user {} already exists", this.user);
            }
        }
        meta.setPassword(this.user, this.password);
        return null;
    }

}
