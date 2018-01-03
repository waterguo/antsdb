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
package com.antsdb.saltedfish.storage;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.Options;

import com.antsdb.saltedfish.cpp.BetterCommandLine;
import com.antsdb.saltedfish.util.ExecResult;
import com.antsdb.saltedfish.util.ExecUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class HBaseCountMain extends BetterCommandLine {

    public static void main(String[] args) throws Exception {
        new HBaseCountMain().parseAndRun(args);
    }
    
    @Override
    protected String getCommandName() {
        return "hbase-count <table name...>";
    }

    @Override
    protected void buildOptions(Options options) {
        options.addOption(null, "config", true, "directory of the hbase configuration");
    }

    @Override
    protected void run() throws Exception {
        for (String i:this.cmdline.getArgList()) {
            ArrayList<String> args = new ArrayList<>();
            args.add("hbase");
            if (this.cmdline.hasOption("config")) {
                args.add("--config");
                args.add(this.cmdline.getOptionValue("config"));
            }
            args.add("org.apache.hadoop.hbase.mapreduce.RowCounter");
            args.add(i);
            ExecResult result = ExecUtil.exec(args.toArray(new String[args.size()]));
            if (result.exit != 0) {
                println("error: %d return from hbase rowcounter for table %s", result.exit, i);
                continue;
            }
            String err = new String(result.stderr);
            Matcher m = Pattern.compile("ROWS=([0-9]+)").matcher(err);
            if (!m.find()) {
                println(err);
                continue;
            }
            long count = Long.parseLong(m.group(1));
            println("%s: %d", i, count);
        }
    }

}
