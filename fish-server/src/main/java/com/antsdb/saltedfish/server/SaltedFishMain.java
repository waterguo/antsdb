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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.antsdb.saltedfish.util.CommandLineHelper;

/**
 * main entry of the saltedfish server
 * 
 * @author *-xguo0<@
 *
 */
public class SaltedFishMain extends CommandLineHelper {
    
    int port;
    ConfigService configService;
    
    private static Options getOptions() {
        Options options = new Options();
        options.addOption("h", "help", false, "print help");
        options.addOption(null, "wait", false, "wait for console");
        return options;
    }
    
    public static void main(String[] args) throws Exception {
        new SaltedFishMain().run(getOptions(), args);
    }
    
    private void run(Options options, String[] args) throws Exception {
        CommandLine line = parse(options, args);
        if (line.hasOption("wait")) {
            println("press anykey to continue ...");
            System.in.read();
        }
        if (line.getArgList().size() < 1) {
            System.err.println("home directory is not specified");
            System.exit(-1);
        }
        for (String i:line.getArgList()) {
            File home = new File(i);
            if (!home.isDirectory()) {
                println("home directory is not valid: {}", home);
                System.exit(-1);
            }
            SaltedFish fish = new SaltedFish(home);
            fish.run();
        }
    }
}
