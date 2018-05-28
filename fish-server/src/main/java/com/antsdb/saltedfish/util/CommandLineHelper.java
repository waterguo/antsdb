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
package com.antsdb.saltedfish.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.varia.NullAppender;

/**
 * 
 * @author wgu0
 */
public class CommandLineHelper implements ConsoleHelper {
    
    static {
        Logger.getRootLogger().removeAllAppenders();
        Logger.getRootLogger().addAppender(new NullAppender());
    }
    
    protected String getCommandName() {
        return this.getClass().getSimpleName();
    }

    public CommandLine parse(Options options, String[] args) throws ParseException {
		CommandLineParser parser = new DefaultParser();
        options.addOption("h", "help", false, "help");
		CommandLine line = parser.parse(options, args);
		if (line.hasOption('h')) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(getCommandName(), options);
            System.exit(0);
		}
		return line;
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
