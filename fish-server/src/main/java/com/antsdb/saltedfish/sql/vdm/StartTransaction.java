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
package com.antsdb.saltedfish.sql.vdm;

public class StartTransaction extends Instruction {
    Instruction next;
    
    public StartTransaction() {
    }
    
    public StartTransaction(Instruction next) {
        super();
        this.next = next;
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        	if (ctx.getSession().isAutoCommit()) {
        		ctx.getSession().resetAutoCommitAfterTrx();
            ctx.session.setAutoCommit(false);
        	}
        	else {
        		ctx.session.commit();
        	}
        ctx.session.startTrx();
        ctx.getTransaction().getGuaranteedTrxId();
        if (this.next != null) {
            return this.next.run(ctx, params, pMaster);
        }
        else {
            return null;
        }
    }
}
