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

import java.util.ArrayList;
import java.util.List;

import com.antsdb.saltedfish.sql.vdm.Parameters;
import com.antsdb.saltedfish.sql.vdm.VdmContext;
import com.antsdb.saltedfish.sql.vdm.View;
import com.antsdb.saltedfish.util.CursorUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class SystemViewClusterStatus extends View {
    
    public class Line {
        public String ENDPOINT;
        public String STATE;
        public Long SERVER_ID;
        public Long LOG_POINTER;
        public Boolean LEADER;
        public String OPTIONS = "";
        public String GOSSIP;
    }
    
    public SystemViewClusterStatus() {
        super(CursorUtil.toMeta(Line.class));
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        List<Line> result = new ArrayList<>();
        long temp = ctx.getHumpback().getServerId();
        try {
            temp = ctx.getOrca().getBelugaPod().getLeaderId();
        }
        catch (Exception x) {};
        final long leaderId = temp;
        
        // add local node
        
        Line line = new Line();
        line.ENDPOINT = ctx.getHumpback().getEndpoint();
        line.SERVER_ID = ctx.getHumpback().getServerId();
        line.STATE = ctx.getOrca().getBelugaPod().isInCluster() ? "CLUSTER" : "SINGLE";
        line.LEADER = line.SERVER_ID == leaderId;
        result.add(line);
        
        // the rest
        
        ctx.getOrca().getBelugaPod().getMembers().forEach((it)->{
           Line ii = new Line();
           ii.ENDPOINT = it.endpoint;
           ii.STATE = it.getState().toString();
           ii.GOSSIP = it.getGossip();
           ii.LOG_POINTER = it.getLogPointer();
           ii.SERVER_ID = it.serverId;
           ii.LEADER = ii.SERVER_ID == leaderId;
           ii.OPTIONS = it.getOptions();
           result.add(ii);
        });
        
        return CursorUtil.toCursor(this.meta, result);
    }

}
