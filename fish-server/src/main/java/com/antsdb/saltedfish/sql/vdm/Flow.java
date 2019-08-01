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

import java.util.ArrayList;
import java.util.List;

public class Flow extends Instruction {
    List<Instruction> instructions = new ArrayList<>();

    public Flow() {
    }
    
    public Flow(List<? extends Instruction> instructions) {
        this.instructions.addAll(instructions);
    }
    
    public void add(Instruction instruction) {
        this.instructions.add(instruction);
    }

    @Override
    public Object run(VdmContext ctx, Parameters params, long pMaster) {
        Object result = null;
        for (Instruction i:this.instructions) {
            Object r = i.run(ctx, params, pMaster);
            if ((result instanceof Integer) && (r instanceof Integer)) {
                result = ((Integer)result) + ((Integer)r);
            }
            else if ((result instanceof Long) && (r instanceof Long)) {
                result = ((Long)result) + ((Long)r);
            }
            else {
                result = r;
            }
        }
        return result;
    }

    @Override
    public void explain(int level, List<ExplainRecord> records) {
        for (Instruction i:instructions) {
            i.explain(level, records);
        }
    }

    public List<Instruction> getInstructions() {
        return this.instructions;
    }
    
}
