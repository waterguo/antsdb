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

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.time.DurationFormatUtils;

import com.antsdb.saltedfish.cpp.FishSkipList;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.nosql.Gobbler.DeleteEntry;
import com.antsdb.saltedfish.nosql.Gobbler.IndexEntry;
import com.antsdb.saltedfish.nosql.Gobbler.InsertEntry;
import com.antsdb.saltedfish.nosql.Gobbler.PutEntry;
import com.antsdb.saltedfish.nosql.Gobbler.UpdateEntry;
import com.antsdb.saltedfish.sql.FishCommandLine;

/**
 * validates the database files
 *  
 * @author wgu0
 */
public class Validator extends FishCommandLine {

	private File home;
	private long count = 0;
	private long startTime;
	private long endTime;
	private boolean doesValidateReplay = false;
	private Humpback humpback;
	private SpaceManager spaceman;
	private long errors = 0;

	public static void main(String[] args) throws Exception {
		new Validator(args).run();
	}

	Validator(String[] args) throws ParseException {
		super(args);
	}
	
    @Override
    protected String getName() {
        return "validator";
    }
    
	protected Options getOptions() {
		Options options = new Options();
		options.addOption("h", "help", false, "print help");
		options.addOption(null, "home", true, "database data directory");
		options.addOption(null, "replay", false, "validtes log replay");
		return options;
	}
	
	private void run() throws Exception {
		CommandLine line = this.cmd;

		// help
		
		if (line.hasOption("help")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("antsdb-validate", getOptions());
			return;
		}

		// now options
		
		println("log location: %s", this.home);
		if (line.hasOption("replay")) {
			this.doesValidateReplay = true;
		}
		println("replay validation: %b", this.doesValidateReplay);
		
		// now go go go
		
		this.humpback = getHumpbackReadOnly();
		if (this.humpback == null) {
			return;
		}
		this.spaceman = this.humpback.getSpaceManager();
		this.startTime = System.currentTimeMillis();
		validateTablets();
		this.endTime = System.currentTimeMillis();
		report();
	}

	private void report() {
		long duration = this.endTime - this.startTime;
        if (duration == 0) {
            return;
        }
		println("errors found: %s", this.errors);
		println("duration: %s", DurationFormatUtils.formatDurationHMS(duration));
		println("speed: %d/sec", this.count * 1000 / duration);
	}

	/**
	 * validate the records in tablets can be found in the data log
	 * @throws IOException 
	 */
	private void validateTablets() throws Exception {
		for (GTable table:this.humpback.getTables()) {
			for (MemTablet tablet:table.getMemTable().getTabletsReadOnly()) {
				validateTablet(tablet);
			}
		}
	}

	private boolean validateTablet(MemTablet tablet) throws Exception {
		Set<Long> positions = new HashSet<>();
		println(tablet.getFile().toString());
		FishSkipList skip = tablet.getSkipList();
		int oHead = skip.getHead();
		long base = tablet.base;
		for (int oNode = oHead; oNode != 0; oNode = FishSkipList.Node.getNext(base, oNode)) {
			long ppll = FishSkipList.Node.getValuePointer(base, oNode);
			int pll = Unsafe.getInt(ppll);
			for (MemTablet.ListNode j = MemTablet.ListNode.create(base, pll); j!=null; j=j.getNextNode()) {
				long version = j.getVersion();
				if (version < -10) {
					this.errors++;
					println("error: negtive version %d is found at %08x", version, j.getOffset());
				}
				long sprow = j.getSpacePointer();
				if (!j.isRow()) {
					continue;
				}
				if (validateRow(sprow)) {
					long spLog = sprow - Gobbler.ENTRY_HEADER_SIZE;
					if (this.doesValidateReplay) {
						positions.add(spLog);
					}
				}
				this.count++;
			}
		}
		
		validateReplay(positions);
		return true;
	}

	private boolean validateRow(long sprow) {
		try {
			long pRow = this.spaceman.toMemory(sprow);
			int magic = Unsafe.getShort(pRow - Gobbler.ENTRY_HEADER_SIZE);
			if (magic != Gobbler.MAGIC) {
				this.errors++;
				println("error: invalid sprow 0x%08x, magic not found", sprow);
				return false;
			}
			long version = Row.getVersion(pRow);
			Row row = Row.fromMemoryPointer(pRow, version);
			int keyOffset = row.getKeyOffset();
			if ((keyOffset <= 0) || (keyOffset >= 1000)) {
				this.errors++;
				println("error: invalid key offset 0x%04x @%0x08x", keyOffset, sprow);
				return false;
			}
			return true;
		}
		catch (Exception x) {
			this.errors++;
			println("error: invalid sprow %08x", sprow);
			x.printStackTrace();
			return false;
		}
	}

	private void validateReplay(Set<Long> positions) throws Exception {
		if (!this.doesValidateReplay) {
			return;
		}
		Gobbler gobbler = this.humpback.getGobbler();
		gobbler.replay(SpaceManager.HEADER_SIZE, true, new ReplayHandler(){

			@Override
            public void insert(InsertEntry entry) throws Exception {
                positions.remove(entry.getSpacePointer());
            }

            @Override
            public void update(UpdateEntry entry) throws Exception {
                positions.remove(entry.getSpacePointer());
            }

            @Override
            public void put(PutEntry entry) throws Exception {
                positions.remove(entry.getSpacePointer());
            }

			@Override
			public void index(IndexEntry entry) {
				positions.remove(entry.getSpacePointer());
			}

			@Override
			public void delete(DeleteEntry entry) {
				long sp = entry.getSpacePointer();
				positions.remove(sp);
			}
		});
		if (positions.size() > 0) {
			for (Long i:positions) {
				this.errors++;
				println("error: log position %08x is not found in replay", i);
			}
		}
	}
}
