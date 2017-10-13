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
package com.antsdb.saltedfish.nosql;

import java.io.File;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class HumpbackUtil {
    static Logger _log = UberUtil.getThisLogger();
    static boolean _isFakeDeletionEnabled = false;
    
    public static void deleteHumpbackFile(File file) {
        if (!_isFakeDeletionEnabled) {
            if (!file.delete()) {
                _log.warn("unable to delete file: {}", file);
            }
            return;
        }
        String path = file.getAbsolutePath();
        File junk = new File(path + ".junk");
        if (!file.renameTo(junk)) {
            _log.warn("unable to rename file {} to {}", file, junk);
        }
    }
}
