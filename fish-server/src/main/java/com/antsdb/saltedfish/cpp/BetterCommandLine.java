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
package com.antsdb.saltedfish.cpp;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.util.ConsoleHelper;

/**
 * 
 * @author *-xguo0<@
 */
public abstract class BetterCommandLine implements ConsoleHelper {
    protected String[] args;
    protected CommandLine cmdline;
    
    protected String getCommandName() {
        return getClass().getSimpleName();
    }
    
    protected void buildOptions(Options options) {
    }
    
    abstract protected void run() throws Exception;
    
    protected void parseAndRun(String[] args) throws Exception {
        this.args = args;
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        buildOptions(options);
        options.addOption("h", "help", false, "help");
        cmdline = parser.parse(options, args);
        if (cmdline.hasOption('h')) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(getCommandName(), options);
            System.exit(0);
        }
        run();
    }

    protected int parseInteger(String value) {
        int result = 0;
        if (value.startsWith("0x")) {
            value = StringUtils.substring(value, 2);
            result = Integer.parseInt(value, 16);
        }
        else {
            result = Integer.parseInt(value);
        }
        return result;
    }

    protected long parseLong(String value) {
        long result = 0;
        if (value.startsWith("0x")) {
            value = StringUtils.substring(value, 2);
            result = Long.parseLong(value, 16);
        }
        else {
            result = Long.parseLong(value);
        }
        return result;
    }
}
