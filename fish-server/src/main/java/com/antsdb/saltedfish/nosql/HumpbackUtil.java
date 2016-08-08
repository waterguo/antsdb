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

import static java.lang.System.err;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.time.DurationFormatUtils;

import com.antsdb.saltedfish.cpp.Bytes;
import com.antsdb.saltedfish.nosql.Gobbler.EntryType;
import com.antsdb.saltedfish.nosql.Gobbler.LogEntry;
import com.antsdb.saltedfish.util.CommandLineHelper;

/**
 * 
 * @author wgu0
 */
public class HumpbackUtil implements CommandLineHelper {

	public static void main(String[] args) throws Exception {
		new HumpbackUtil().run(args);
	}

	private File home;

	private Options getOptions() {
		Options options = new Options();
		options.addOption("h", "help", false, "print help");
		options.addOption(null, "validate", false, "validates data");
		options.addOption(null, "home", true, "antsdb home directory");
		return options;
	}

	private void run(String[] args) throws Exception {
		CommandLine line = parse(getOptions(), args);
		
		if (line.getOptions().length == 0 || line.hasOption('h')) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("humpback-util", getOptions());
			return;
		}
		
		// options
		
		if (line.getOptionValue("home") == null) {
			err.println("error: data directory is not specified");
			return;
		}
		this.home = new File(line.getOptionValue("home"));
		if (!home.isDirectory()) {
			err.println("error: invalid home directory");
			return;
		}
		
		// commands
		
		if (line.hasOption("validate")) {
			validate();
		}
	}

	private void validate() throws Exception {
		long start = System.currentTimeMillis();
		int failures = 0;
		Humpback humpback = Humpback.open(this.home);
		for (GTable table : humpback.getTables()) {
			println("validating %s ...", table.toString());
			try {
				Boolean isIndex = null;
				RowIterator scanner = table.scan(0, Long.MAX_VALUE);
				while (scanner.next()) {
					if (isIndex == null) {
						isIndex = isIndex(humpback, scanner.getRowPointer());
					}
					if (!isIndex) {
						Row row = scanner.getRow();
						for (int i=0; i<row.getMaxColumnId(); i++) {
							row.get(i);
						}
					}
					else {
						long pKey = scanner.getRowKeyPointer();
						Bytes.get(null, pKey);
					}
				}
			}
			catch (Exception x) {
				failures++;
				x.printStackTrace();
			}
		}
		println("tables failed validation: %d", failures);
		long duration = System.currentTimeMillis() - start;
		println("duration: %s", DurationFormatUtils.formatDurationHMS(duration));
	}

	private Boolean isIndex(Humpback humpback, long sp) {
		sp -= 6;
		long p = humpback.getSpaceManager().toMemory(sp);
		LogEntry entry = new LogEntry(sp, p);
		EntryType type = entry.getType();
		return type == EntryType.INDEX;
	}

}
