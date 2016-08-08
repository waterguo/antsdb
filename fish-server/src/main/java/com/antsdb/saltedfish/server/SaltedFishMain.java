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
package com.antsdb.saltedfish.server;

import java.io.File;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

/**
 * main entry of the saltedfish server
 * 
 * @author *-xguo0<@
 *
 */
public class SaltedFishMain {
    static Logger _log = UberUtil.getThisLogger();
    
    int port;
    ConfigService configService;
    
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("home directory is not specified");
            System.exit(-1);
        }
        for (String i:args) {
            File home = new File(i);
            if (!home.isDirectory()) {
                System.err.println("home directory is not found");
                System.exit(-1);
            }
            SaltedFish fish = new SaltedFish(home);
            fish.run();
        }
    }
}
