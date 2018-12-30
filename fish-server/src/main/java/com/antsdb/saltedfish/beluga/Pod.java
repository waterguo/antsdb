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
package com.antsdb.saltedfish.beluga;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.HumpbackSession;
import com.antsdb.saltedfish.nosql.LogDependency;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class Pod implements LogDependency {
    static final Logger _log = UberUtil.getThisLogger();
    
    List<Member> members = Collections.synchronizedList(new ArrayList<>());
    Orca orca; 
    private Quorum quorum;
    private HumpbackSession hsession;
    private SlaveWarmer warmer;
    volatile boolean warm = false;
    
    public Pod(Orca orca) {
        this.orca = orca;
        this.hsession = orca.getHumpback().createSession("local/pod");
        this.warmer = new SlaveWarmer(this);
    }
    
    public synchronized void add(Member member) {
        for (Member i:this.members) {
            if (i.endpoint.equalsIgnoreCase(member.endpoint)) {
                throw new OrcaException("node {} is already in the cluster", member.endpoint);
            }
        }
        BelugaThread thread = new BelugaThread(this.orca, member);
        member.thread = thread;
        this.members.add(member);
        try  {
            Humpback humpback = this.orca.getHumpback();
            this.hsession.open();
            member.save(humpback, hsession, getPrefix());
            save(humpback, hsession);
        }
        finally {
            this.hsession.close();
        }
        refreshWarmFlag();
        thread.start();
    }

    public void start() {
        for (Member member:this.members) {
            BelugaThread thread = new BelugaThread(this.orca, member);
            member.thread = thread;
            thread.start();
        }
        this.warmer.start();
    }
    
    private String getPrefix() {
        return "/" + this.orca.getHumpback().getServerId() + "/cluster/";
    }
    
    private void load() {
        Humpback humpback = this.orca.getHumpback();
        String prefix = getPrefix();
        String members = humpback.getConfig(prefix + "members");
        if (members == null) {
            return;
        }
        for (String i:StringUtils.split(members, ",")) {
            Member member = new Member();
            member.serverId = Long.parseLong(i);
            member.load(humpback, prefix);
            this.members.add(member);
        }
        refreshWarmFlag();
    }
    
    private void save(Humpback humpback, HumpbackSession hsession) {
        String members = "";
        for (Member i:this.members) {
            if (!members.isEmpty()) {
                members += ",";
            }
            members += String.valueOf(i.serverId);
        }
        humpback.setConfig(hsession, getPrefix() + "members", members);
    }

    public List<Member> getMembers() {
        return Collections.unmodifiableList(this.members);
    }

    public synchronized void delete(String endpoint) {
        for (Member i:new ArrayList<>(this.members)) {
            if (i.endpoint.equalsIgnoreCase(endpoint)) {
                i.stop();
                this.members.remove(i);
                Humpback humpback = this.orca.getHumpback();
                HumpbackSession hsession=humpback.createSession("local/pod");
                try  {
                    save(humpback, hsession);
                }
                finally {
                    humpback.deleteSession(hsession);
                }
                return;
            }
        }
        refreshWarmFlag();
        throw new OrcaException("endpoint {} is not found", endpoint); 
    }
    
    public void open() throws Exception {
        load();
        if (getConfigAsBoolean("quorum")) {
            openQuorum();
        }
    }

    public void close() {
        for (Member i:this.members) {
            i.stop();
        }
        if (this.quorum != null) {
            this.quorum.close();
        }
    }

    @Override
    public String getName() {
        return "cluster";
    }

    @Override
    public List<LogDependency> getChildren() {
        if (this.members.size() == 0) {
            return Collections.emptyList();
        }
        List<LogDependency> result = new ArrayList<>();
        this.members.forEach(it->result.add(it));
        return result;
    }

    public synchronized void join() throws Exception {
        openQuorum();
        this.quorum.register();
        try {
            this.hsession.open();
            setConfig(hsession, "quorum", true);
        }
        finally {
            this.hsession.close();
        }
    }
    
    private void openQuorum() throws Exception {
        if (this.quorum == null) {
            this.quorum = new Quorum(this);
            this.quorum.open();
        }
    }

    public void leave() throws Exception {
        if (this.quorum == null) {
            return;
        }
        this.quorum.unregister();
        this.quorum.close();
        this.quorum = null;
        for (Member i:new ArrayList<>(this.members)) {
            delete(i.endpoint);
        }
        try {
            this.hsession.open();
            setConfig(hsession, "quorum", false);
        }
        finally {
            this.hsession.close();
        }
    }

    public Member findMemberById(long id) {
        for (Member i:this.members) {
            if (i.serverId == id) {
                return i;
            }
        }
        return null;
    }
    
    private void setConfig(HumpbackSession hsession, String key, Object value) {
        Humpback humpback = this.orca.getHumpback();
        humpback.setConfig(this.hsession, getPrefix() + key, value);
    }
    
    private boolean getConfigAsBoolean(String key) {
        Humpback humpback = this.orca.getHumpback();
        return humpback.getConfigAsBoolean(getPrefix() + key, false);
    }

    public long getLeaderId() throws Exception {
        return (this.quorum == null) ? this.orca.getHumpback().getServerId() : this.quorum.getLeaderId();
    }
    
    public boolean isInCluster() {
        return this.quorum != null;
    }

    public boolean isLeader() {
        return (this.quorum == null) ? true : this.quorum.isLeader();
    }

    public SlaveWarmer getWarmer() {
        return this.warmer;
    }
    
    private void refreshWarmFlag() {
        this.warm = false;
        for (Member i:this.members) {
            if (i.warmer) {
                this.warm = true;
                break;
            }
        }
    }
}
