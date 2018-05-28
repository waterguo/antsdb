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

import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.UserMeta;

/**
 * 
 * @author *-xguo0<@
 */
public class DropUser extends Instruction {

    private String user;
    private boolean ifExists;

    public DropUser(String user, boolean ifExists) {
        this.user = user;
        this.ifExists = ifExists;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        MetadataService meta = ctx.getOrca().getMetaService();
        UserMeta userMeta = meta.getUser(this.user);
        if (userMeta == null) {
            if (this.ifExists) {
                return null;
            }
            throw new OrcaException("user {} does not exist", this.user);
        }
        meta.dropUser(user);
        return null;
    }

}
