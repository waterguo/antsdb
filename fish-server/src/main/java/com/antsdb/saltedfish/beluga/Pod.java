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

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;

import com.antsdb.saltedfish.minke.Minke;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.HumpbackSession;
import com.antsdb.saltedfish.nosql.LogDependency;
import com.antsdb.saltedfish.server.mysql.ErrorMessage;
import com.antsdb.saltedfish.slave.DbUtils;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.util.UberUtil;
import com.google.common.collect.MapDifference;
import com.google.common.collect.MapDifference.ValueDifference;
import com.google.common.collect.Maps;

/**
 * 
 * @author *-xguo0<@
 */
public class Pod implements LogDependency {
    static final Logger _log = UberUtil.getThisLogger();
    
    Map<Long, Member> members = Collections.synchronizedMap(new HashMap<>());
    Orca orca; 
    private Quorum quorum;
    private HumpbackSession hsession;
    private SlaveWarmer warmer;
    volatile boolean warm = false;
    private boolean isLeader = false;

    private long lpLocalStart;
    
    public Pod(Orca orca) {
        this.orca = orca;
        this.hsession = orca.getHumpback().createSession("local/pod");
        this.warmer = new SlaveWarmer(this);
    }
    
    public synchronized void add(Member member) {
    }

    private String getPrefix() {
        return "/" + this.orca.getHumpback().getServerId() + "/cluster/";
    }
    
    public List<Member> getMembers() {
        return new ArrayList<>(this.members.values());
    }

    public void open() throws Exception {
        if (this.orca.getHumpback().getConfig().isClusterEnabled()) {
            openQuorum();
            refresh(this.orca.getSpaceManager().getAllocationPointer());
        }
    }

    public void close() {
        for (Member i:this.members.values()) {
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
        getMembers().forEach(it->result.add(it));
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
        if (this.isLeader()) {
            this.giveUpLeader();
        }
        this.quorum.leave(this.orca.getHumpback().getServerId());
    }

    public Member findMemberById(long id) {
        return this.members.get(id);
    }
    
    private void setConfig(HumpbackSession hsession, String key, Object value) {
        Humpback humpback = this.orca.getHumpback();
        humpback.setConfig(this.hsession, getPrefix() + key, value);
    }
    
    public Long getLeaderId() throws Exception {
        return (this.quorum == null) ? null : this.quorum.getLeaderId();
    }
    
    public boolean isInCluster() {
        return this.quorum != null;
    }

    public boolean isLeader() {
        return this.isLeader;
    }

    public SlaveWarmer getWarmer() {
        return this.warmer;
    }
    
    public void activate(long serverId) throws Exception {
        // target node must be standby
        QuorumNode node = this.quorum.getNode(serverId);
        if (node == null) {
            throw new OrcaException("node {} is not found", serverId);
        }
        if (node.state != BelugaState.STANDBY) {
            throw new OrcaException("node {} is not in STANDBY mode", serverId);
        }
        
        String url = String.format("jdbc:mysql://%s", node.endpoint);
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url);
        }
        catch (Exception x) {
            throw new OrcaException("unable to reach node {}", node.endpoint);
        }
        try {
            // target node must be empty
            Map<String, Object> row = null;
            if (this.orca.getHumpback().getStorageEngine() instanceof Minke) {
                try {
                    row = DbUtils.firstRow(conn, "SELECT * FROM antsdb.x0 WHERE table_id>=100");
                }
                catch (Exception x) {
                    throw new OrcaException("unable to validate node {}", node.endpoint);
                }
                if (row != null) {
                    throw new OrcaException("target node must be empty in order to be activiated");
                }
            }
            
            // verify server id
            try {
                row = DbUtils.firstRow(conn, "SELECT 1 WHERE @@server_id=?", node.serverId);
            }
            catch (Exception x) {
            }
            if (row == null) {
                throw new ErrorMessage(0, "unexpected server id from the slave node", node.serverId);
            }
            
            // empty cache, slave node needs to reload everything from remote storage
            if (!(this.orca.getHumpback().getStorageEngine() instanceof Minke)) {
                DbUtils.execute(conn, ".evict cache all");
            }
            
            // promote the node, if this is the first active node in the cluster, make it active other else
            // we need to load
            for (QuorumNode i:this.quorum.getNodes().values()) {
                if (i.state == BelugaState.ACTIVE) {
                    if (this.orca.getHumpback().getStorageEngine() instanceof Minke) {
                        this.quorum.setState(serverId, BelugaState.LOADING);
                    }
                    else {
                        this.quorum.setState(serverId, BelugaState.ACTIVE);
                    }
                    return;
                }
            }
            
            // this is the first active node 
            this.quorum.setState(serverId, BelugaState.ACTIVE);
        }
        finally {
            DbUtils.closeQuietly(conn);
        }
    }

    public BelugaState getState(long serverId) throws Exception {
        QuorumActiveNode status = this.quorum.getActiveNode(serverId);
        return status == null ? BelugaState.STANDBY : status.state;
    }

    public Quorum getQuorum() {
        return this.quorum;
    }

    public void onBecomeMaster() throws Exception {
        try {
            Map<Long,QuorumNode> nodes = this.quorum.getNodes();
            QuorumNode qnode = nodes.get(this.orca.getHumpback().getServerId());
            if (qnode == null || qnode.state != BelugaState.ACTIVE) {
                // inactive node can't become master
                return;
            }
            long lp = this.orca.getHumpback().getSpaceManager().getAllocationPointer();
            _log.info("becoming master {}", lp);
            this.isLeader = true;
            this.orca.setSlaveMode(false);
            refresh(lp);
            Thread.sleep(Long.MAX_VALUE);
        }
        catch (Exception x) {
            _log.error("error", x);
            throw x;
        }
        finally {
            _log.info("becoming slave");
            this.isLeader = false;
            this.orca.setSlaveMode(true);
        }
    }

    public void onBecomeSlave() {
        try {
            this.orca.setSlaveMode(true);
        }
        catch (Exception x) {
            _log.error("error", x);
        }
        try {
            // wait 2 s for replication to finish
            UberUtil.sleep(2000);
            stopReplication();
        }
        catch (Exception x) {
            _log.error("error", x);
        }
    }
    
    public void onActiveNodesChange() {
        try {
            refresh(this.orca.getHumpback().getSpaceManager().getAllocationPointer());
        }
        catch (Exception x) {
            _log.error("error", x);
        }
    }

    private long getServerId() {
        return this.orca.getHumpback().getServerId();
    }
    
    private synchronized void refresh(long lp) throws Exception {
        Map<Long, QuorumNode> nodes = this.quorum.getNodes();
        
        // enter leader election if we are active
        this.quorum.enableElection(nodes.get(getServerId()).state == BelugaState.ACTIVE);
        
        // manage replications
        if (this.isLeader()) {
            refreshReplciation(lp);
        }
        else {
            stopReplication();
        }
    }

    private synchronized void refreshReplciation(long lp) throws Exception {
        Map<Long, QuorumNode> nodes = this.quorum.getNodes();
        MapDifference<Long, Object> diff = Maps.difference(this.members, nodes);
        
        // activate new pending active nodes 
        for (Map.Entry<Long, Object> i:diff.entriesOnlyOnRight().entrySet()) {
            Member m = new Member(this);
            m.qnode = (QuorumNode) i.getValue();
            _log.debug("found member: {}", m.getName());
            if (m.qnode.state != BelugaState.STANDBY && m.getServerId() != this.orca.getHumpback().getServerId()) {
                startReplication(m, lp);
                this.members.put(i.getKey(), m);
            }
        };
        
        // remove deleted nodes
        diff.entriesOnlyOnLeft().forEach((key,obj)->{
            Member m = this.members.remove(key);
            m.stop();
        });
        
        // remove nodes being put into STANDBY
        for (Entry<Long, ValueDifference<Object>> i:diff.entriesDiffering().entrySet()) {
            Member m = (Member) i.getValue().leftValue();
            QuorumNode qnode = (QuorumNode) i.getValue().rightValue();
            if (qnode.state == BelugaState.STANDBY) {
                this.members.remove(i.getKey());
                m.stop();
            }
        }
    }

    private void startReplication(Member member, long lp) throws Exception {
        // mark is mandatory because replicator start point is non-inclusive
        this.orca.getHumpback().getGobbler().logMessage(null, "replication mark: " + member.getEndpoint());
        BelugaThread thread = new BelugaThread(this.orca, member);
        member.thread = thread;
        member.lp = lp;
        thread.start();
    }

    private void stopReplication() {
        for (Member i:this.members.values()) {
            i.stop();
        }
        this.members.clear();
    }

    public void giveUpLeader() {
        if (!this.isLeader()) {
            throw new OrcaException("current database node is not leader");
        }
        this.quorum.giveUpLeader();
    }

    public void delete(long serverId) throws Exception {
        this.quorum.delete(serverId);
    }

    /** local start log pointer of the  uncommitted changes */
    public void setStartReplicationLogPointer(long value) {
        this.lpLocalStart = value;
    }

    public long getStartReplicationLogPointer() {
        return this.lpLocalStart;
    }

    public Member getMember(long serverId) {
        return this.members.get(serverId);
    }
}
