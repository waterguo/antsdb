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

import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.mapreduce.RowCounter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

/**
 * 
 * @author *-xguo0<@
 */
public class HBaseCountMain extends HBaseCommandLine {

    static MyAppender _appender = new MyAppender();
    
    static {
        // Logger.getLogger(Configuration.class).setLevel(Level.OFF);
        Logger.getLogger(Job.class).setLevel(Level.INFO);
        Logger.getLogger(Job.class).addAppender(_appender);
    }
    
    static class MyAppender extends AppenderSkeleton {
        long result = 0;
        
        @Override
        public void close() {
        }

        @Override
        public boolean requiresLayout() {
            return false;
        }

        @Override
        protected void append(LoggingEvent event) {
            String message = event.getMessage().toString();
            int idx = message.indexOf("ROWS=");
            if (idx <= 0) {
                return;
            }
            int idx2 = message.indexOf('\n', idx);
            result = Long.parseLong(message.substring(idx + 5, idx2));
        }
    }
    
    public HBaseCountMain(String[] args) throws ParseException {
        super(args);
    }

    public static void main(String[] args) throws Exception {
        new HBaseCountMain(args).run();
    }

    @Override
    protected Options getOptions() {
        return new Options();
    }

    
    @Override
    protected String getCommandName() {
        return "hbase-count <table name...>";
    }

    private void run() throws Exception {
        Configuration conf = getConfiguration();
        for (String i:this.parser.getArgList()) {
            String[] args = new String[] {i};
            try {
                Job job = RowCounter.createSubmittableJob(conf, args);
                job.waitForCompletion(true);
                println("%s: %d", i, _appender.result);
            }
            catch (TableNotFoundException x) {
                println("%s: not found", i);
            }
            catch (Exception x) {
                x.printStackTrace();
            }
        }
    }

}
