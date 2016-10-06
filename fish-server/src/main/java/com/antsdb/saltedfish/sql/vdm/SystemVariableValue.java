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
package com.antsdb.saltedfish.sql.vdm;

import java.util.function.Consumer;

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.cpp.Int4;
import com.antsdb.saltedfish.cpp.Unicode16;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.Orca;

/**
 * this is mysql specific
 * 
 * @author xinyi
 *
 */
public class SystemVariableValue extends Operator {
    String name;
    
    public SystemVariableValue(String name) {
        super();
        this.name = name;
    }

    @Override
    public long eval(VdmContext ctx, Heap heap, Parameters params, long pRecord) {
        if (this.name.equalsIgnoreCase("@@session.auto_increment_increment")) {
            return Int4.allocSet(heap, 1);
        }
        else if (this.name.equalsIgnoreCase("@@version")) {
        	return Unicode16.allocSet(heap, Orca.VERSION);
        }
        else if (this.name.equalsIgnoreCase("@@version_comment")) {
        	return Unicode16.allocSet(heap, "Source distribution");
        }
        else {
            return 0;
        }
    }

    @Override
    public DataType getReturnType() {
        if (this.name.equalsIgnoreCase("@@session.auto_increment_increment")) {
            return DataType.longtype();
        }
        else if (this.name.equalsIgnoreCase("@@version_comment")) {
        	return DataType.varchar(57);
        }
        else {
            return DataType.varchar();
        }
    }

    @Override
    public void visit(Consumer<Operator> visitor) {
        visitor.accept(this);
    }
}
