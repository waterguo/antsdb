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
import java.util.function.Consumer;

public abstract class Function extends Operator {
    List<Operator> parameters = new ArrayList<Operator>();
    
    @Override
    public List<Operator> getChildren() {
        return this.parameters;
    }

    public void addParameter(Operator op) {
        this.parameters.add(op);
    }

    @Override
    public void visit(Consumer<Operator> visitor) {
        visitor.accept(this);
        for (Operator i:parameters) {
            if (i != null) {
                i.visit(visitor);
            }
        }
    }

    public abstract int getMinParameters();
    
    public int getMaxParameters() {
        return getMinParameters();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName().substring(4);
    }
}
