/*-------------------------------------------------------------------------------------------------
 _______ __   _ _______ _______ ______  ______
 |_____| | \  |    |    |______ |     \ |_____]
 |     | |  \_|    |    ______| |_____/ |_____]

 Copyright (c) 2016, antsdb.com and/or its affiliates. All rights reserved. *-xguo0<@

 This program is free software: you can redistribute it and/or modify it under the terms of the
 GNU Affero General Public License, version 3, as published by the Free Software Foundation.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/agpl-3.0.txt>
-------------------------------------------------------------------------------------------------*/
package com.antsdb.saltedfish.sql;

import com.antsdb.saltedfish.sql.meta.SequenceMeta;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.Transaction;
import com.antsdb.saltedfish.util.IdGenerator;


public class IdentifierService {
	static Transaction _trx = new Transaction(1, Long.MAX_VALUE);
	static final ObjectName GLOBAL_SEQUENCE_NAME = new ObjectName(Orca.SYSNS, "GlobalId");
	static int  GLOBAL_SEQUENCE_ID = 0;
	
    Orca orca;
    SequenceMeta global;

    IdentifierService(Orca orca) {
        super();
        this.orca = orca;
        this.global = this.orca.getMetaService().getSequence(_trx, GLOBAL_SEQUENCE_NAME);
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
    
    public long getNextGlobalId(int increment) {
        long result = this.global.getLastNumber();
        result += increment;
        this.global.setLastNumber(result);
        this.orca.getMetaService().updateSequence(_trx.getTrxId(), this.global);
        return result;
    }
    
    public long getNextId(ObjectName name) {
        return getNextId(name, 0);
    }
    
    public long getNextId(ObjectName name, int increment) {
        SequenceMeta seq = this.orca.getMetaService().getSequence(_trx, name);
        long counter;
        if (seq == null) {
            seq = new SequenceMeta(this.orca, name);
            seq.setLastNumber(0l);
            counter = 0;
        }
        else {
            counter = seq.getLastNumber();
            counter += (increment == 0) ? seq.getIncrement() : increment;
            seq.setLastNumber(counter);
            this.orca.getMetaService().updateSequence(seq.getTransactionTimestamp(), seq);
        }
        return counter;
    }
    
}
