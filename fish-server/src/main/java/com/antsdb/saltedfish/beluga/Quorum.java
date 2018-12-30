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
import java.util.HashSet;
import java.util.Set;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.ConfigService;
import com.antsdb.saltedfish.server.mysql.ErrorMessage;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class Quorum implements Watcher, ConnectionStateListener {
    static final Logger _log = UberUtil.getThisLogger();
    
    private ConfigService config;
    private boolean enabled;
    private Orca orca;
    private CuratorFramework client;
    private LeaderSelector selector;
    private Pod pod;

    public Quorum(Pod pod) {
        this.pod = pod;
        this.orca = pod.orca;
        this.config = orca.getHumpback().getConfig();
    }

    public void open() throws Exception {
        if (this.config.getZookeeperConnectionString() == null) {
            return;
        }
        
        // connect to zookeeper
        
        _log.info("connecting to zookeeper: {}", this.config.getZookeeperConnectionString());
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        this.client = CuratorFrameworkFactory.newClient(this.config.getZookeeperConnectionString(), retryPolicy);
        this.client.start();
        this.client.createContainers("/antsdb/election");
        this.client.createContainers("/antsdb/nodes");
        this.client.getConnectionStateListenable().addListener(this);
        
        // leader election
        
        LeaderSelectorListener listener = new LeaderSelectorListenerAdapter() {
            public void takeLeadership(CuratorFramework client) throws Exception {
                _log.info("taking leader");
                try {
                    Quorum.this.orca.setSlaveMode(false);
                    for (;;) {
                        Thread.sleep(1000);
                    }
                }
                finally {
                    Quorum.this.orca.setSlaveMode(true);
                    _log.info("leader released");
                }
            }
        };
        this.selector = new LeaderSelector(client, "/antsdb/election", listener);
        this.selector.setId(String.valueOf(this.orca.getHumpback().getServerId()));
        selector.autoRequeue();
        selector.start();
        
        // done
        
        initZookeeper();
        refreshNodes();
        this.enabled = true;
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
        String path = "/antsdb/nodes/" + this.orca.getHumpback().getServerId();
        if (this.client.checkExists().forPath(path) != null) {
            throw new ErrorMessage(ErrorMessage.ZK_NODE_ALREADY_REGISTERED, "node is already registered in zookeeper");
        }
        String data = this.orca.getHumpback().getEndpoint();
        this.client.create().withMode(CreateMode.PERSISTENT).forPath(path, data.getBytes());
    }
    
    public void unregister() throws Exception {
        if (!this.isEnabled()) {
            throw new ErrorMessage(ErrorMessage.ZK_NOT_ENABLED, "zookeeper is not enabled");
        }
        String path = "/antsdb/nodes/" + this.orca.getHumpback().getServerId();
        this.client.delete().forPath(path);
    }
    
    private void refreshNodes() throws Exception {
        Set<Long> set = new HashSet<>();
        for (String i:this.client.getChildren().usingWatcher(this).forPath("/antsdb/nodes")) {
            long ii = Long.parseLong(i);
            set.add(ii);
            if (ii == this.orca.getHumpback().getServerId()) {
                continue;
            }
            if (this.pod.findMemberById(ii) != null) {
                return;
            }
            byte[] bytes = this.client.getData().forPath("/antsdb/nodes/" + i);
            Member member = new Member();
            member.serverId = ii;
            member.endpoint = new String(bytes);
            member.load = this.orca.isLeader() ? true : false;
            member.init = true;
            this.pod.add(member);
        }
        for (Member i:new ArrayList<>(this.pod.members)) {
            if (!set.contains(i.serverId)) {
                this.pod.delete(i.endpoint);
            }
        }
    }
    
    @Override
    public void process(WatchedEvent event) {
        try {
            if (event.getType() == EventType.NodeChildrenChanged) {
                refreshNodes();
            }
        }
        catch (Exception x) {
            _log.error("error from zookeeper", x);
        }
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        try {
            if (newState == ConnectionState.CONNECTED) {
                initZookeeper();
            }
            else if (newState == ConnectionState.RECONNECTED) {
                refreshNodes();
            }
        }
        catch (Exception x) {
            _log.error("error from zookeeper", x);
        }
    }

    private void initZookeeper() throws Exception {
        _log.info("zookeeper is connected");
        this.client.getChildren().usingWatcher(this).forPath("/antsdb/nodes");
        refreshNodes();
        // elect();
    }

    public long getLeaderId() throws Exception {
        return Long.parseLong(this.selector.getLeader().getId());
    }

    public boolean isLeader() {
        return this.selector.hasLeadership();
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
}
