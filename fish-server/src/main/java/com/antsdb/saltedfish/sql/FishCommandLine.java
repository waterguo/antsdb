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
package com.antsdb.saltedfish.sql;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.varia.NullAppender;

import com.antsdb.saltedfish.nosql.HumpbackReadOnly;
import com.antsdb.saltedfish.util.CommandLineHelper;

/**
 * 
 * @author wgu0
 */
public abstract class FishCommandLine implements CommandLineHelper {
	protected CommandLine cmd;
	
	abstract protected Options getOptions();
	
	static {
		String level = System.getenv("ANTSDB_DEBUG");
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
		this.cmd = parse(getOptions(), args);
	}
	
	public File getHome() {
		String home = cmd.getOptionValue("home");
		if (home == null) {
			home = System.getenv("ANTSDB_HOME");
		}
		if (home == null) {
			println("error: home directory is not specified");
			return null;
		}
		File file = new File(home);
		if (!new File(file, "data/checkpoint.bin").exists()) {
			println("error: home directory '%s' is invalid", file.getAbsolutePath());
			return null;
		}
		return file;
	}
	
	public HumpbackReadOnly getHumpbackReadOnly() throws Exception {
		File home = getHome();
		if (home == null) {
			return null;
		}
		HumpbackReadOnly humpback = new HumpbackReadOnly(home);
		return humpback;
	}
}
