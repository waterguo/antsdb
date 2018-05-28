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

import static com.antsdb.saltedfish.sql.OrcaConstant.TABLEID_SYSCOLUMN;
import static com.antsdb.saltedfish.sql.OrcaConstant.TABLEID_SYSPARAM;
import static com.antsdb.saltedfish.sql.OrcaConstant.TABLEID_SYSRULE;
import static com.antsdb.saltedfish.sql.OrcaConstant.TABLEID_SYSSEQUENCE;
import static com.antsdb.saltedfish.sql.OrcaConstant.TABLEID_SYSTABLE;
import static com.antsdb.saltedfish.sql.OrcaConstant.TABLEID_SYSUSER;

import java.util.Base64;
import java.util.Random;

import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.SlowRow;
import com.antsdb.saltedfish.nosql.TableType;
import com.antsdb.saltedfish.sql.meta.ColumnId;
import com.antsdb.saltedfish.sql.meta.MetadataService;
import com.antsdb.saltedfish.sql.meta.SequenceMeta;
import com.antsdb.saltedfish.sql.vdm.KeyMaker;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
class Seed {
    private static final Logger _log = UberUtil.getThisLogger();
    
    private Humpback humpback;
    private Orca orca;
    private boolean updated = false;

    Seed(Orca orca) {
        this.orca = orca;
        this.humpback = orca.getHumpback();
    }
    
    void run() {
        // system tables
        
        createSystemTable(TABLEID_SYSSEQUENCE);
        createSystemTable(TABLEID_SYSTABLE);
        createSystemTable(TABLEID_SYSCOLUMN);
        createSystemTable(TABLEID_SYSPARAM);
        createSystemTable(TABLEID_SYSRULE);
        createSystemTable(TABLEID_SYSUSER);
        
        // system sequence 
        
        createSystemSequence(IdentifierService.GLOBAL_SEQUENCE_NAME, 0, 0x100, 1);
        createSystemSequence(IdentifierService.ROWID_SEQUENCE_NAME, 1, 0, 1);
        
        // default config
        
        setConfig("databaseType", this.humpback.getConfig().getDefaultDatabaseType());
        setConfig("antsdb_auth_seed", genSeed());
        
        // done
        
        if (this.updated) {
            _log.info("seed upgrade is completed");
        }
    }
    
    private String genSeed() {
        /* seed captured from mysql
        byte[] seed  = new byte[] {
                0x50, 0x3a, 0x6e, 0x3d, 0x25, 0x40, 0x51, 0x56, 0x73, 0x68, 
                0x2f, 0x50, 0x27, 0x6f, 0x7a, 0x38, 0x46, 0x38, 0x26, 0x51};
        */
        byte[] bytes = new byte[15];
        new Random(System.currentTimeMillis()).nextBytes(bytes);
        byte[] seed = Base64.getEncoder().encode(bytes);
        return new String(seed);
    }
    
    private void setConfig(String key, String value) {
        key = key.toLowerCase();
        GTable sysparam = this.humpback.getTable(TABLEID_SYSPARAM);
        if (sysparam.get(0, Long.MAX_VALUE, KeyMaker.make(key)) != 0) {
            return;
        }
        notifyUpgrade();
        SlowRow row = new SlowRow(key);
        row.set(ColumnId.sysparam_name.getId(), key);
        row.set(ColumnId.sysparam_value.getId(), value);
        sysparam.put(1, row, 0);
    }

    private void createSystemTable(int tableId) {
        if (this.humpback.getTable(tableId) != null) {
            return;
        }
        notifyUpgrade();
        this.humpback.createTable(Orca.SYSNS, String.format("x%08x", tableId), tableId, TableType.DATA);
    }

    private void createSystemSequence(ObjectName name, int id, long next, int increment) {
        MetadataService meta = this.orca.getMetaService();
        SequenceMeta seq = meta.getSequence(Transaction.getSeeEverythingTrx(), name);
        if (seq != null) {
            return;
        }
        notifyUpgrade();
        seq = new SequenceMeta(name, id);
        seq.setLastNumber(next);
        seq.setIncrement(increment);
        meta.addSequence(Transaction.getSystemTransaction(), seq);
    }
    
    private void notifyUpgrade() {
        if (this.updated) {
            return;
        }
        this.updated = true;
        _log.info("start upgrading seed ...");
    }
}
