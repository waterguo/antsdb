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

import java.util.Properties;

import com.antsdb.saltedfish.beluga.Pod;
import com.antsdb.saltedfish.sql.OrcaException;

/**
 * 
 * @author *-xguo0<@
 */
public class ActivateNode extends Statement {

    private Properties props;
    private long serverId;

    public ActivateNode(long serverId, Properties props) {
        this.serverId = serverId;
        this.props = props;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params) {
        Pod pod = ctx.getOrca().getBelugaPod();
        try {
            pod.activate(this.serverId);
        }
        catch (Exception x) {
            throw new OrcaException(x);
        }
        return null;
    }
}
