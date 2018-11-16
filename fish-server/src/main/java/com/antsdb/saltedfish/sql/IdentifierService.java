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

import java.util.concurrent.atomic.AtomicLong;

import com.antsdb.saltedfish.nosql.HumpbackSession;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.SequenceMeta;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.util.IdGenerator;


public class IdentifierService {
    static Transaction _trx = new Transaction(1, Long.MAX_VALUE);
    static final ObjectName GLOBAL_SEQUENCE_NAME = new ObjectName(Orca.SYSNS, "GlobalId");
    static final ObjectName ROWID_SEQUENCE_NAME = new ObjectName(Orca.SYSNS, "rowid");
    static int  GLOBAL_SEQUENCE_ID = 0;
    
    Orca orca;
    AtomicLong nextRowid = new AtomicLong();
    volatile long rowidEnd;
    MetadataService meta;
    HumpbackSession hsession;

    IdentifierService(Orca orca) {
        super();
        this.orca = orca;
        this.meta = orca.getMetaService();
        SequenceMeta seq = this.orca.getMetaService().getSequence(_trx, ROWID_SEQUENCE_NAME);
        this.nextRowid.set(seq.getLastNumber());
        this.rowidEnd = this.nextRowid.get();
        this.hsession = this.orca.getHumpback().createSession();
    }
    
    /**
     * get a time base unique id. not sequential. but always larger
     * 
     * @return
     */
    public long getTimeId() {
        return IdGenerator.getId();
    }
    
    /**
     * global sequential id. expensive
     * 
     * @return
     */
    public long getNextGlobalId() {
        return getNextGlobalId(1);
    }
    
    public long getNextRowid() {
        for (;;) {
            long result = this.nextRowid.getAndIncrement();
            if (result >= this.rowidEnd) {
                allocRowidBlock();
                continue;
            }
            return result;
        }
    }
    
    private synchronized void allocRowidBlock() {
        try (HumpbackSession foo = this.hsession.open()) {
            this.rowidEnd = this.nextRowid.get() + 1000000;
            SequenceMeta seq = this.orca.getMetaService().getSequence(_trx, ROWID_SEQUENCE_NAME); 
            seq.setLastNumber(rowidEnd);
            long version = this.orca.getTrxMan().getNewVersion();
            this.orca.metaService.updateSequence(hsession, version, seq);
            seq.getRow().setTrxTimestamp(version);
        }
    }
    
    public long getNextGlobalId(int increment) {
        return getNextId(GLOBAL_SEQUENCE_NAME, increment);
    }
    
    public long getNextId(ObjectName name) {
        try (HumpbackSession foo = this.hsession.open()) {
            return this.meta.nextSequence(this.hsession, name);
        }
    }
    
    public long getNextId(ObjectName name, int increment) {
        try (HumpbackSession foo = this.hsession.open()) {
            return this.meta.nextSequence(this.hsession, name, increment);
        }
    }
    
    void close() {
        SequenceMeta seq = this.orca.getMetaService().getSequence(_trx, ROWID_SEQUENCE_NAME); 
        seq.setLastNumber(this.nextRowid.get());
        try (HumpbackSession foo = this.hsession.open()) {
            this.orca.metaService.updateSequence(this.hsession, this.orca.getTrxMan().getNewVersion(), seq);
        }
        this.orca.getHumpback().deleteSession(this.hsession);
    }
}
