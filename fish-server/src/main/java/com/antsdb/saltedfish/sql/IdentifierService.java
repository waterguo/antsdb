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
	
    Orca orca;

    IdentifierService(Orca orca) {
        super();
        this.orca = orca;
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
    public long getSequentialId() {
        return getSequentialId(new ObjectName(Orca.SYSNS, "system"));
    }
    
    public long getSequentialId(ObjectName name) {
        SequenceMeta seq = this.orca.getMetaService().getSequence(_trx, name);
        long counter;
        if (seq == null) {
            seq = new SequenceMeta(this.orca, name);
            seq.setLastNumber(0l);
            counter = 0;
        }
        else {
            counter = seq.getLastNumber();
            counter++;
            seq.setLastNumber(counter);
            this.orca.getMetaService().updateSequence(seq.getTransactionTimestamp(), seq);
        }
        return counter;
    }
    
}
