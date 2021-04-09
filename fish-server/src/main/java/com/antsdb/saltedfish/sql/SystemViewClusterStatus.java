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

import com.antsdb.saltedfish.beluga.Pod;
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
        public Boolean LEADER;
        public Boolean ONLINE;
        public Long LOG_POINTER;
        public String OPTIONS = "";
        public String GOSSIP;
    }
    
    public SystemViewClusterStatus() {
        super(CursorUtil.toMeta(Line.class));
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        List<Line> result = new ArrayList<>();
        Long temp = null;
        try {
            temp = ctx.getOrca().getBelugaPod().getLeaderId();
        }
        catch (Exception x) {};
        Long leaderId = temp;
        
        // the rest
        try {
            Pod pod = ctx.getOrca().getBelugaPod();
            /*
            pod.getMembers().forEach((it)->{
               Line ii = new Line();
               ii.ENDPOINT = it.getEndpoint();
               ii.STATE = String.valueOf(it.getState());
               ii.GOSSIP = it.getGossip();
               ii.LOG_POINTER = it.getLogPointer();
               ii.SERVER_ID = it.getServerId();
               ii.LEADER = ii.SERVER_ID == leaderId;
               ii.OPTIONS = it.getOptions();
               result.add(ii);
            });
            */
            pod.getQuorum().getNodes().values().forEach((it)->{
                Line ii = new Line();
                ii.ENDPOINT = it.endpoint;
                ii.STATE = it.state.toString();
                //ii.GOSSIP = it.getGossip();
                //ii.LOG_POINTER = it.getLogPointer();
                ii.SERVER_ID = it.serverId;
                ii.ONLINE = it.online;
                ii.LEADER = leaderId == null ? null : (long)ii.SERVER_ID == (long)leaderId;
                //ii.OPTIONS = it.getOptions();
                result.add(ii);
             });
        }
        catch (Exception e) {
            throw new OrcaException(e);
        }
        
        return CursorUtil.toCursor(this.meta, result);
    }

}
