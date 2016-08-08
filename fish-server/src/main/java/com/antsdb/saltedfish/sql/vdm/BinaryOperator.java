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

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public abstract class BinaryOperator extends Operator {
    public Operator left;
    public Operator right;

    public BinaryOperator(Operator left, Operator right) {
        super();
        this.left = left;
        this.right = right;
    }

    @Override
    public List<Operator> getChildren() {
        return Arrays.asList(new Operator[]{left, right});
    }

    @Override
    public void visit(Consumer<Operator> visitor) {
        visitor.accept(this);
        if (this.left != null) {
            this.left.visit(visitor);
        }
        if (this.right != null) {
            this.right.visit(visitor);
        }
    }
}
