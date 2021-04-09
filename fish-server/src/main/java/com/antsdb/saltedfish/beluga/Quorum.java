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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.ConfigService;
import com.antsdb.saltedfish.server.mysql.ErrorMessage;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * wrapper of the zookeeper
 * 
 * @author *-xguo0<@
 */
public class Quorum {
    static final Logger _log = UberUtil.getThisLogger();
    
    private ConfigService config;
    private boolean enabled;
    private Orca orca;
    private CuratorFramework client;
    private LeaderSelector selector;
    private boolean isSelectorStarted;
    private Pod pod;
    private String name;
    private String pathNodes;
    private String pathElection;
    private String pathActive;
    private String pathOnline;

    public Quorum(Pod pod) {
        this.pod = pod;
        this.orca = pod.orca;
        this.config = orca.getHumpback().getConfig();
        this.name = this.config.getClusterName();
        this.pathElection = "/antsdb/" + name + "/election";
        this.pathNodes = "/antsdb/" + name + "/nodes";
        this.pathActive = "/antsdb/" + name + "/active";
        this.pathOnline = "/antsdb/" + name + "/online";
    }

    public void open() throws Exception {
        if (this.config.getZookeeperConnectionString() == null) {
            return;
        }
        
        // connect to zookeeper
        _log.info("connecting to zookeeper: {}", this.config.getZookeeperConnectionString());
        RetryPolicy retryPolicy = new RetryOneTime(1000);
        this.client = CuratorFrameworkFactory.newClient(this.config.getZookeeperConnectionString(), retryPolicy);

        //
        this.client.start();
        this.client.createContainers(this.pathNodes);
        this.client.createContainers(this.pathElection);
        this.client.createContainers(this.pathActive);
        this.client.createContainers(this.pathOnline);
        this.enabled = true;
        register();
        onConnect();

        // connection events
        this.client.getConnectionStateListenable().addListener((client, state)->{
            if (state == ConnectionState.CONNECTED) {
                onConnect();
            }
            else if (state == ConnectionState.RECONNECTED) {
                onReconnect();
            }
        });
        
        // leader election
        this.selector = new LeaderSelector(client, this.pathElection, new LeaderSelectorListenerAdapter() {
            public void takeLeadership(CuratorFramework client) throws Exception {
                onTakeLeadership();
            }
        });
        this.selector.setId(String.valueOf(this.orca.getHumpback().getServerId()));
        this.isSelectorStarted = false;
        
        // done
        _log.info("zookeeper is connected");
        this.enabled = true;
    }

    void enableElection(boolean value) throws Exception {
        if (value) {
            if (!this.isSelectorStarted) {
                this.selector.autoRequeue();
                this.selector.start();
                this.isSelectorStarted = true;
            }
        }
        else {
            if (this.isSelectorStarted) {
                this.selector.close();
                this.isSelectorStarted = false;
                this.selector = new LeaderSelector(client, "/antsdb/election", new LeaderSelectorListenerAdapter() {
                    public void takeLeadership(CuratorFramework client) throws Exception {
                        onTakeLeadership();
                    }
                });
            }
        }
    }
    
    protected void onTakeLeadership() throws Exception {
        _log.info("taking leader");
        try {
            Quorum.this.pod.onBecomeMaster();
        }
        finally {
            Quorum.this.pod.onBecomeSlave();
            _log.info("leader released");
        }
    }

    private void onReconnect() {
        onConnect();
    }

    private void onConnect() {
        _log.info("zookeeper is connected to {}", this.config.getZookeeperConnectionString());
        try {
            // watch for active nodes
            watch(this.pathActive, ()->{
                onActiveNodesChange();
                return null; 
            });
        }
        catch (Exception x) {
            _log.error("unable to set watch", x);
        }
    }

    private void onActiveNodesChange() {
        this.pod.onActiveNodesChange();
    }

    public boolean isEnabled() {
        return this.enabled;
    }

    public void close () {
        this.client.close();
    }

    public void register() throws Exception {
        if (!this.isEnabled()) {
            throw new ErrorMessage(ErrorMessage.ZK_NOT_ENABLED, "zookeeper is not enabled");
        }
        QuorumNode node = new QuorumNode();
        node.serverId = this.orca.getHumpback().getServerId();
        node.endpoint = this.orca.getHumpback().getConfig().getAuxEndpoint();
        setObject(CreateMode.PERSISTENT, this.pathNodes + "/" + getServerId(), node);
        setObject(CreateMode.EPHEMERAL, this.pathOnline + "/" + getServerId(), "");
    }
    
    private long getServerId() {
        return this.orca.getHumpback().getServerId();
    }
    
    public void unregister() throws Exception {
        if (!this.isEnabled()) {
            throw new ErrorMessage(ErrorMessage.ZK_NOT_ENABLED, "zookeeper is not enabled");
        }
        String path = "/antsdb/nodes/" + this.orca.getHumpback().getServerId();
        this.client.delete().forPath(path);
    }
    
    public Long getLeaderId() throws Exception {
        if (this.selector == null) {
            return null;
        }
        Participant leader = this.selector.getLeader();
        if (leader == null) {
            return null;
        }
        String leaderId = leader.getId();
        if (StringUtils.isEmpty(leaderId)) {
            return null;
        }
        return Long.parseLong(leaderId);
    }

    public boolean isLeader() {
        return (this.selector != null) ? this.selector.hasLeadership() : false;
    }

    public Map<Long,QuorumNode> getNodes() throws NumberFormatException, Exception {
        Map<Long,QuorumNode> result = new HashMap<>();
        for (String i:this.client.getChildren().forPath(this.pathNodes)) {
            QuorumNode node = getNode(Long.parseLong(i));
            result.put(node.serverId, node);
        }
        return result;
    }

    public QuorumNode getNode(long serverId) throws Exception {
        QuorumNode node = getObject(this.pathNodes + "/" + serverId, QuorumNode.class);
        if (node != null) {
            BelugaState state = getObject(this.pathActive + "/" + serverId, BelugaState.class);
            node.state = state == null ? BelugaState.STANDBY : state;
            node.online = this.client.checkExists().forPath(this.pathOnline + "/" + serverId) != null;
        }
        return node;
    }

    private <T> T getObject(String path, Class<T> klass) throws Exception {
        try {
            byte[] bytes = this.client.getData().forPath(path);
            String s = new String(bytes);
            return UberUtil.toObject(s, klass);
        }
        catch (NoNodeException x) {
            return null;
        }
    }
    
    private void setObject(CreateMode mode, String path, Object value) throws Exception {
        String json = UberUtil.toJson(value);
        this.client.create().orSetData().withMode(mode).forPath(path, json.getBytes());
    }
    
    public QuorumActiveNode getActiveNode(long serverId) throws Exception {
        try {
            byte[] bytes = this.client.getData().forPath(this.pathActive + "/" + serverId);
            QuorumActiveNode node = UberUtil.toObject(new String(bytes), QuorumActiveNode.class);
            return node;
        }
        catch (NoNodeException x) {
            return null;
        }
    }

    void watch(String path, Callable<Object> call) throws Exception {
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    call.call();
                }
                catch (Exception x) {
                    
                }
                finally {
                    try {
                        Quorum.this.client.getChildren().usingWatcher(this).forPath(path);
                    }
                    catch (Exception ignored) {
                    }
                }
            }
        };
        this.client.getChildren().usingWatcher(watcher).forPath(path);
    }
    
    public void setState(long serverId, BelugaState state) throws Exception {
        if (getNode(serverId) == null) {
            throw new OrcaException("node {} is not found", serverId); 
        }
        String path = this.pathActive + "/" + serverId;
        setObject(CreateMode.PERSISTENT, path, state);
    }

    public void activate(long serverId) throws Exception {
    }

    public void leave(long serverId) throws Exception {
        String path = this.pathActive + "/" + serverId;
        if (this.client.checkExists().forPath(path) == null) {
            throw new OrcaException("node {} is not active", serverId);
        }
        this.client.delete().forPath(path);
    }
    
    /*
    private void createIfNotExist(final String path, byte data[], CreateMode createMode) 
    throws KeeperException, InterruptedException {
        try {
            this.zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
        }
        catch (KeeperException.NodeExistsException x) {
        }
    }
    
    private String getElectionPath() {
        long serverId = this.orca.getHumpback().getServerId();
        return String.format("/antsdb/election/%016x_", serverId);
    }
    
    public void elect() throws KeeperException, InterruptedException {
        for (;;) {
            ChildrenCallback callback = (int rc, String path, Object ctx, List<String> nodes)-> {
                Collections.sort(nodes); 
                String leader = nodes.get(0);
                long leaderServerId = Long.parseLong(StringUtils.split(leader, '_')[0], 16);
                if (leaderServerId == this.orca.getHumpback().getServerId()) {
                    this.orca.setSlaveMode(false);
                    return;
                }
                if (this.zk.exists(leader, true) == null) {
                    // leader is gone at this very moment, retry
                    continue;
                }
                this.orca.setSlaveMode(true);
            };
            Watcher watcher = (event)-> {
                elect();
            };
            this.zk.getChildren("/antsdb/election", watcher, callback, null);
        }
    }
    */

    public void giveUpLeader() {
        this.selector.interruptLeadership();
    }

    private void delete(String path) throws Exception {
        this.client.delete().quietly().forPath(path);
    }
    
    public void delete(long serverId) throws Exception {
        Long leader = getLeaderId();
        if (leader != null && leader == serverId) {
            throw new OrcaException("leader cannot be deleted");
        }
        if (getNode(serverId) == null) {
            throw new OrcaException("node {} is not found", serverId);
        }
        this.delete(this.pathElection + "/" +serverId);
        this.delete(this.pathActive + "/" + serverId);
        this.delete(this.pathNodes + "/" + serverId);
    }
}
