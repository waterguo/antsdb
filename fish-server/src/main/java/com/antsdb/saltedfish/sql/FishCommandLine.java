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
package com.antsdb.saltedfish.sql;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.varia.NullAppender;

import com.antsdb.saltedfish.minke.Minke;
import com.antsdb.saltedfish.minke.MinkeCache;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.nosql.StorageEngine;
import com.antsdb.saltedfish.nosql.SysMetaRow;
import com.antsdb.saltedfish.util.CommandLineHelper;

/**
 * 
 * @author wgu0
 */
public abstract class FishCommandLine extends CommandLineHelper {
	protected CommandLine cmd;
	
	abstract protected Options getOptions();
	abstract protected String getName();
	
	static {
		String level = System.getenv("ANTSDB_LOG_LEVEL");
		BasicConfigurator.resetConfiguration();
		if (level != null) {
			BasicConfigurator.resetConfiguration();
			BasicConfigurator.configure();
			Logger.getRootLogger().setLevel(Level.toLevel(level));
		}
		else {
			BasicConfigurator.resetConfiguration();
			BasicConfigurator.configure(new NullAppender());
		}
	}
	
	public FishCommandLine(String[] args) throws ParseException {
	    Options options = getOptions();
        options.addOption("h", "help", false, "print help");
        options.addOption(null, "home", true, "antsdb home");
		this.cmd = parse(options, args);
	}
	
	public File getHome() {
		String home = cmd.getOptionValue("home");
		if (home == null) {
			home = System.getenv("ANTSDB_HOME");
		}
		if (home == null) {
			println("error: home directory is not specified");
			System.exit(-1);
		}
		File file = new File(home);
		if (!new File(file, "data").isDirectory()) {
			println("error: home directory '%s' is invalid", file.getAbsolutePath());
            System.exit(-1);
		}
		return file;
	}
	
	public Humpback getHumpbackReadOnly() throws Exception {
		File home = getHome();
		if (home == null) {
			return null;
		}
		Humpback humpback = new Humpback(home);
		humpback.setMutable(false);
		humpback.open();
		return humpback;
	}
	
	public Minke getMinke() throws Exception  {
	    StorageEngine storage = getHumpbackReadOnly().getStorageEngine();
	    if (storage instanceof Minke) {
	        return (Minke)storage;
	    }
        if (storage instanceof MinkeCache) {
            return ((MinkeCache)storage).getMinke();
        }
        return null;
	}
	
	/**
	 * find the table either by name or id
	 * @param name
	 * @return
	 * @throws Exception 
	 */
    protected GTable findTable(String name) throws Exception {
        String ns = null;
        String[] words = StringUtils.split(name, '.');
        Humpback humpback = getHumpbackReadOnly();
        if (words.length == 2) {
            ns = words[0].toLowerCase();
            name = words[1].toLowerCase();
            for (SysMetaRow i:humpback.getTablesMeta()) {
                if (i.isDeleted()) {
                    continue;
                }
                if (ns != null) {
                    if (!ns.equals(i.getNamespace().toLowerCase())) {
                        continue;
                    }
                    if (!name.equals(i.getTableName().toLowerCase())) {
                        continue;
                    }
                }
                return humpback.getTable(i.getTableId());
            }
        }
        else {
            int tableId = parseInteger(name);
            return humpback.getTable(tableId);
        }
        return null;
    }
}
