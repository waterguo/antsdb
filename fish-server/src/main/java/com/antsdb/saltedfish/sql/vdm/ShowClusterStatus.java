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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class ShowClusterStatus extends View {
    
    public class Line {
        public String ENDPOINT;
        public String STATE;
        public Long SERVER_ID;
        public Long LOG_POINTER;
        public String OPTIONS;
        public String GOSSIP;
    }
    
    public ShowClusterStatus() {
        super(CursorUtil.toMeta(Line.class));
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        try {
            List<Line> result = new ArrayList<>();
            
            // add local node
            
            Line line = new Line();
            String port = ctx.getHumpback().getConfig().getProperty("fish.port", "3306");
            line.ENDPOINT = InetAddress.getLocalHost().getHostName() + ":" + port;
            line.SERVER_ID = ctx.getHumpback().getServerId();
            line.STATE = null;
            result.add(line);
            
            // the rest
            
            ctx.getOrca().getBelugaPod().getMembers().forEach((it)->{
               Line ii = new Line();
               ii.ENDPOINT = it.getEndpoint();
               ii.STATE = it.getState().toString();
               ii.GOSSIP = it.getGossip();
               ii.LOG_POINTER = it.getLogPointer();
               ii.SERVER_ID = it.getServerId();
               ii.OPTIONS = it.getOptions();
               result.add(ii);
            });
            
            return CursorUtil.toCursor(this.meta, result);
        }
        catch (UnknownHostException x) {
            throw new OrcaException(x);
        }
    }

}
