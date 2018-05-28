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

import java.util.List;
import java.util.function.Consumer;

import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.OrcaException;

public class BindParameter extends Operator {

    int pos;
    
    public BindParameter(int pos) {
        super();
        this.pos = pos;
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        long pValue = 0;
        if (this.pos >= params.size()) {
            throw new OrcaException("parameter {} is missing", this.pos) ;
        }
        if (params instanceof FishParameters) {
            pValue = ((FishParameters) params).getAddress(this.pos);
        }
        else {
            pValue = FishObject.allocSet(heap, params.get(this.pos));
        }
        return pValue;
    }

    @Override
    public DataType getReturnType() {
        return null;
    }

    @Override
    public List<Operator> getChildren() {
        return null;
    }

    @Override
    public void visit(Consumer<Operator> visitor) {
        visitor.accept(this);
    }
    
    @Override
    public String toString() {
        return "?" + this.pos;
    }
}
