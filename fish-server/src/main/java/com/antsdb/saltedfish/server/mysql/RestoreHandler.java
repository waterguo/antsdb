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
package com.antsdb.saltedfish.server.mysql;

import com.antsdb.mysql.network.PacketFishRestoreEnd;
import com.antsdb.mysql.network.PacketFishRestoreStart;
import com.antsdb.saltedfish.cpp.FlexibleHeap;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.TrxMan;
import com.antsdb.saltedfish.nosql.VaporizingRow;
import com.antsdb.saltedfish.server.fishnet.PacketFishRestore;
import com.antsdb.saltedfish.sql.Session;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.Checks;
import com.antsdb.saltedfish.sql.vdm.ObjectName;

/**
 * 
 * @author *-xguo0<@
 */
public class RestoreHandler {

    private Session session;
    private GTable gtable;
    private Heap heap;
    private TableMeta table;
    private long trxid;

    public RestoreHandler(Session session) {
        this.session = session;
    }

    public void prepare(PacketFishRestoreStart packet) {
        ObjectName name = ObjectName.parse(packet.getTableFullName());
        this.table = Checks.tableExist(this.session, name);
        this.gtable = this.session.getOrca().getHumpback().getTable(table.getId());
        this.heap = new FlexibleHeap();
        this.trxid = this.session.getOrca().getHumpback().getTrxMan().getNewTrxId();
    }

    public void restore(PacketFishRestore packet) {
        this.heap.reset(0);
        VaporizingRow vrow = new VaporizingRow(heap, this.table.getMaxColumnId());
        Row row = Row.fromMemoryPointer(packet.getRowAddress(), this.trxid);
        if (row.getMaxColumnId() != vrow.getMaxColumnId()) {
            throw new IllegalArgumentException();
        }
        vrow.setKey(row.getKeyAddress());
        vrow.setVersion(this.trxid);
        for (int i=0; i<=row.getMaxColumnId(); i++) {
            vrow.setFieldAddress(i, row.getFieldAddress(i));
        }
        this.gtable.insert(this.session.getHSession(), vrow, 0);
    }

    public void end(PacketFishRestoreEnd packet) {
        if (this.heap != null) {
            this.heap.close();
            this.heap = null;
        }
        if (this.trxid != 0) {
            TrxMan trxman = this.session.getOrca().getHumpback().getTrxMan();
            this.session.getOrca().getHumpback().getTrxMan().commit(this.trxid, trxman.getNewVersion());
            this.trxid = 0;
        }
    }

}
