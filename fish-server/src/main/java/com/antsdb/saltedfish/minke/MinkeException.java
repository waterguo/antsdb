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
package com.antsdb.saltedfish.minke;

import java.io.IOException;

import com.antsdb.saltedfish.util.FishException;

/**
 * 
 * @author *-xguo0<@
 */
public class MinkeException extends FishException {

    private static final long serialVersionUID = 1L;

    public MinkeException(IOException x) {
        super(x);
    }

    public MinkeException(Exception x, String message, Object[] params) {
        super(x, message, params);
    }

    public MinkeException(String message, Object... params) {
        super(null, message, params);
    }

}
