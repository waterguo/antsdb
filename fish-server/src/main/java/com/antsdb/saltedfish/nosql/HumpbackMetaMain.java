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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.util.StringUtils;

import com.antsdb.saltedfish.sql.FishCommandLine;

/**
 * 
 * @author *-xguo0<@
 */
public class HumpbackMetaMain extends FishCommandLine {
    
    private HumpbackReadOnly humpback;
    
    HumpbackMetaMain(String[] args) throws ParseException {
        super(args);
    }
    
    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption("h", "help", false, "print help");
        options.addOption("n", "name", true, "table id, name or wildcard");
        options.addOption(null, "home", true, "antsdb home");
        return options;
    }

    public static void main(String[] args) throws Exception {
        new HumpbackMetaMain(args).run();
    }

    private void run() throws Exception {
        CommandLine line = this.cmd;
        
        if (line.getOptions().length == 0 || line.hasOption('h')) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("humpback-meta", getOptions());
            return;
        }
        
        this.humpback = getHumpbackReadOnly();
        if (this.humpback == null) {
            return;
        }
        if (line.hasOption('n')) {
            findTable(line.getOptionValue('n'));
            return;
        }
        println("error: -n is not specified");
    }

    private void findTable(String value) throws Exception {
        if (value.equals("*")) {
            listAllTables();
            return;
        }
        
        if (findTableById(value)) {
            return;
        }
        if (findTableByName(value)) {
            return;
        }
    }

    private boolean findTableByName(String value) throws Exception {
        String[] words = StringUtils.split(value, '.');
        if (words.length != 2) {
            return false;
        }
        String ns = words[0].toLowerCase();
        String table = words[1].toLowerCase();
        for (SysMetaRow i:this.getHumpbackReadOnly().getTablesMeta()) {
            if (!ns.equals(i.getNamespace().toLowerCase())) {
                continue;
            }
            if (!table.equals(i.getTableName().toLowerCase())) {
                continue;
            }
            println(i);
            return true;
        }
        return false;
    }

    private boolean findTableById(String value) {
        try {
            int id = Integer.parseInt(value);
            SysMetaRow row = getHumpbackReadOnly().getTableMeta(id);
            println(row);
            return true;
        }
        catch (Exception x) {
            return false;
        }
    }

    private void listAllTables() throws Exception {
        for (SysMetaRow i:this.getHumpbackReadOnly().getTablesMeta()) {
            println(i);
        }
    }

    private void println(SysMetaRow row) {
        println("%s.%s", row.getNamespace(), row.getTableName());
        println("  id: %d", row.getTableId());
        println("  type: %s", row.getType());
    }
}
