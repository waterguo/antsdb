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

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;

import com.antsdb.mysql.network.LogReplicateRequest;
import com.antsdb.mysql.network.MysqlClient;
import com.antsdb.mysql.network.PacketError;
import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.IndexEntry2;
import com.antsdb.saltedfish.nosql.InsertEntry2;
import com.antsdb.saltedfish.nosql.Replicator;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.nosql.ScanOptions;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.util.UberFormatter;
import com.antsdb.saltedfish.util.UberTime;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
class BelugaThread extends Thread {
    static Logger _log = UberUtil.getThisLogger();
    
    Member member;
    private Orca orca;
    Replicator<PeerReplicator> replicator;
    PeerReplicator peer;
    private Pod pod;

    BelugaThread(Orca orca, Member member) {
        super(member.getEndpoint());
        this.orca = orca;
        this.pod = orca.getBelugaPod();
        this.member = member;
        this.setDaemon(true);
        this.peer = new PeerReplicator(orca, this.member, getPrefix());
    }

    @Override
    public void run() {
        try {
            run0();
        }
        catch (Exception x) {
            _log.error("error", x);
        }
    }

    private String getPrefix() {
        return "/" + this.orca.getHumpback().getServerId() + "/cluster/";
    }
    
    private void run0() throws Exception {
        BelugaState state = this.member.getState();
        if (state == BelugaState.LOADING) {
            load();
        }
        replicate();
    }

    private void replicate() throws Exception {
        Humpback humpback = this.orca.getHumpback();
        this.replicator = new Replicator<>(getName(), humpback, this.peer, true, false);
        this.member.setState(BelugaState.ACTIVE, null);
        this.replicator.run();
    }

    private void load() throws Exception {
        long count = 0;
        long start = UberTime.getTime();
        Humpback humpback = this.orca.getHumpback();
        List<GTable> tables = new ArrayList<>(humpback.getTables());
        Collections.sort(tables, (x,y)->{
            return Integer.compare(x.getId(), y.getId());
        });
        try (BluntHeap heap = new BluntHeap()) {
            try (MysqlClient client = this.peer.createConnection()) {
                load(client, heap, tables.get(Humpback.SYSNS_TABLE_ID));
                for (GTable i:tables) {
                    if (i.getId() != Humpback.SYSNS_TABLE_ID) { 
                        count += load(client, heap, i);
                    }
                }
            }
        }
        this.pod.getQuorum().setState(this.member.getServerId(), BelugaState.ACTIVE);
        long elapse = UberTime.getTime() - start;
        _log.info("load is completed time={} tables={} rows={}", 
                UberFormatter.duration(Duration.ofMillis(elapse)),
                tables.size(),
                count);
    }

    private long load(MysqlClient client, BluntHeap heap, GTable table) throws Exception {
        long count = 0;
        RowIterator j = table.scan(0, Long.MAX_VALUE, 0, 0, ScanOptions.NO_CACHE);
        while (j.next()) {
            heap.reset(0);
            long pEntry = toLogEntry(heap, table.getTableType(), table.getId(), j);
            LogReplicateRequest request = new LogReplicateRequest(0, pEntry);
            client.send(request);
            ByteBuffer response = client.readPacket();
            if (client.isError(response)) {
                PacketError error = new PacketError(response);
                throw new Exception(error.getErrorMessage());
            }
            count++;
        }
        return count;
    }

    private long toLogEntry(BluntHeap heap, TableType tableType, int tableId, RowIterator j) {
        if (tableType == TableType.DATA) {
            InsertEntry2 entry = new InsertEntry2(heap, 0, j.getRow(), tableId);
            return entry.getAddress();
        }
        else {
            long version = j.getVersion();
            long pIndexKey = j.getKeyPointer();
            long pRowKey = j.getRowKeyPointer();
            byte misc = j.getMisc();
            IndexEntry2 entry = IndexEntry2.alloc(heap, 0, tableId, version, pIndexKey, pRowKey, misc);
            return entry.getAddress();
        }
    }

}
