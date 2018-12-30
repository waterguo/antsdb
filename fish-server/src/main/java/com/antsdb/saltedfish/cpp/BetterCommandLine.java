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
package com.antsdb.saltedfish.cpp;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.log4j.Appender;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.PatternLayout;

import com.antsdb.saltedfish.util.ConsoleHelper;
import com.antsdb.saltedfish.util.ParseUtil;

/**
 * 
 * @author *-xguo0<@
 */
public abstract class BetterCommandLine implements ConsoleHelper {
    protected String[] args;
    protected CommandLine cmdline;
    
    public BetterCommandLine() {
        BasicConfigurator.configure();
        LogManager.getRootLogger().setLevel(Level.OFF);
        Appender appender = (Appender)LogManager.getRootLogger().getAllAppenders().nextElement();
        appender.setLayout(new PatternLayout("%d{HH:mm:ss,SSS} [%t] %-5p %c{2} - %m%n"));
    }
    
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
        options.addOption(null, "log-level", true, "ALL, ERROR, WARN, INFO, DEBUG or TRACE. default is OFF");
        cmdline = parser.parse(options, args);
        if (cmdline.hasOption('h')) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(getCommandName(), options);
            System.exit(0);
        }
        if (cmdline.hasOption("log-level")) {
            Level level = Level.toLevel(cmdline.getOptionValue("log-level"));
            LogManager.getRootLogger().setLevel(level);
        }
        run();
    }

    protected int parseInteger(String value) {
        return ParseUtil.parseInteger(value);
    }

    protected long parseLong(String value) {
        return ParseUtil.parseLong(value);
    }
}
