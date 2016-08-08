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

import static java.lang.System.*;

import java.io.File;
import java.util.Base64;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import com.antsdb.saltedfish.cpp.Bytes;
import com.antsdb.saltedfish.cpp.Int8;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.nosql.Gobbler.CommitEntry;
import com.antsdb.saltedfish.nosql.Gobbler.DeleteEntry;
import com.antsdb.saltedfish.nosql.Gobbler.IndexEntry;
import com.antsdb.saltedfish.nosql.Gobbler.RollbackEntry;
import com.antsdb.saltedfish.util.BytesUtil;
import com.antsdb.saltedfish.util.ConsoleHelper;
import com.antsdb.saltedfish.util.JumpException;

/**
 * command line tool that dumps data from the log
 * 
 * @author wgu0
 */
public class LogReplayer extends ReplayHandler implements ConsoleHelper {
	File home;
	MyLogger out = new MyLogger(System.out);
	private boolean hexDump = false;
	private SpaceManager spaceman;
	private Integer tableId;
	private Long rowid;
	private boolean notrx = false;
	private long pKey;
	private long offset = -1;
	private int length = -1;
	
	public static void main(String[] args) throws Exception {
		new LogReplayer().run(args);
	}

	private Options getOptions() {
		Options options = new Options();
		options.addOption("h", "help", false, "print help");
		options.addOption(null, "home", true, "database data directory");
		options.addOption(null, "hex", false, "hex dump of each packet");
		options.addOption(null, "table", true, "table filter");
		options.addOption(null, "rowid", true, "rowid filter");
		options.addOption(null, "notrx", false, "hide trx");
		options.addOption(null, "key", true, "row key");
		options.addOption("s", null, true, "offset");
		options.addOption("n", null, true, "length");
		return options;
	}

	private void run(String[] args) throws Exception {
		CommandLineParser parser = new DefaultParser();
		CommandLine line = parser.parse(getOptions(), args);
		
		// help
		
		if (line.hasOption("help")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("replay", getOptions());
			return;
		}
		
		// other options
		
		this.hexDump  = line.hasOption("hex");
		this.notrx = line.hasOption("notrx");
		if (line.getOptionValue("table") != null) {
			this.tableId = Integer.parseInt(line.getOptionValue("table"));
		}
		if (line.getOptionValue("rowid") != null) {
			this.rowid = Long.parseLong(line.getOptionValue("rowid"));
		}
		if (line.getOptionValue("key") != null) {
			String keyText = line.getOptionValue("key");
			byte[] keyBytes = Base64.getDecoder().decode(keyText);
			this.pKey = Unsafe.allocateMemory(keyBytes.length + 4);
			Bytes.set(this.pKey, keyBytes);
		}
		
		// start offset
		
		if (line.getOptionValue('s') != null) {
			String literal = line.getOptionValue('s');
			this.offset = Long.parseUnsignedLong(literal, 16);
		}
		
		// length
		
		if (line.getOptionValue('n') != null) {
			String literal = line.getOptionValue('n');
			this.length = Integer.parseInt(literal);
		}
		
		// now go
		
		if (line.getOptionValue("home") == null) {
			err.println("error: data directory is not specified");
			return;
		}
		this.home = new File(line.getOptionValue("home"));
		if (!home.isDirectory()) {
			err.println("error: invalid home directory");
			return;
		}
		if (!new File(this.home, "checkpoint.bin").exists()) {
			err.println("error: invalid home directory");
			return;
		}
		
		// go go go
		this.out.println("log location: {}", this.home);
		this.spaceman = new SpaceManager(home, false);
		spaceman.init();
		Gobbler gobbler = new Gobbler(spaceman, false);
		findStart();
		println("start position: %08x", this.offset);
		try {
			gobbler.replay(this.offset, true, this);
		}
		catch (JumpException ignored) {}
	}

	/**
	 * find the start position if it is not pointing at an valid log entry
	 */
	private void findStart() {
		if (this.offset == -1) {
			this.offset = this.spaceman.getStartSp();
			return;
		}
		for (;;) {
			long p = this.spaceman.toMemory(this.offset);
			int magic = Unsafe.getShort(p);
			if (magic == Gobbler.MAGIC) {
				break;
			}
			this.offset++;
		}
	}

	@Override
	public void put(Gobbler.PutEntry entry) {
		long pRow = entry.getRowPointer();
		long sp = entry.getSpacePointer();
		long spRow = entry.getRowSpacePointer();
		int tableId = Row.getTableId(pRow);
		if (this.tableId != null) {
			if (this.tableId != tableId) {
				return;
			}
		}
		long version = Row.getVersion(pRow);
		Row row = Row.fromMemoryPointer(pRow, version);
		if (this.rowid != null) {
			long pRowidFromRow = row.getFieldAddress(0);
			if (pRowidFromRow == 0) {
				return;
			}
			long rowidFromRow = Int8.get(null, pRowidFromRow);
			if (this.rowid != rowidFromRow) {
				return;
			}
		}
		if (this.pKey != 0) {
			if (Bytes.compare(this.pKey, row.getKeyAddress()) != 0) {
				return;
			}
		}
		println("put @ %08x tableId=%d version=%d", sp, tableId, version);
		checkLength();
		if (this.hexDump) {
			long p = this.spaceman.toMemory(spRow);
			String dump = BytesUtil.toHex(p, length);
			this.out.println(dump);
		}
	}

	@Override
	public void index(IndexEntry entry) {
		if (this.tableId != null) {
			if (this.tableId != entry.getTableId()) {
				return;
			}
		}
		if (this.pKey != 0) {
			if (Bytes.compare(this.pKey, entry.getRowKeyAddress()) != 0) {
				return;
			}
		}
		println("index @ %08x tableId=%d trxid=%d", 
				         entry.getSpacePointer(), 
				         entry.getTableId(), 
				         entry.getTrxid());
		checkLength();
	}

	@Override
	public void delete(DeleteEntry entry) {
		int tableId = entry.getTableId();
		long sp = entry.getSpacePointer();
		long trxid = entry.getTrxid();
		
		if (this.tableId != null) {
			if (this.tableId != tableId) {
				return;
			}
		}
		if (this.pKey != 0) {
			if (Bytes.compare(this.pKey, pKey) != 0) {
				return;
			}
		}
		println("delete @ %08x tableId=%d trxid=%d", sp, tableId, trxid);
		checkLength();
	}

	@Override
	public void commit(CommitEntry entry) {
		long sp = entry.getSpacePointer();
		long trxid = entry.getTrxid();
		long trxts = entry.getVersion();
		
		if (this.notrx) {
			return;
		}
		println("commit @ %08x trxid=%d trxts=%d", sp, trxid, trxts);
		checkLength();
	}

	@Override
	public void rollback(RollbackEntry entry) {
		long sp = entry.getSpacePointer();
		long trxid = entry.getTrxid();
		
		if (this.notrx) {
			return;
		}
		println("rollback @ %08x trxid=%d", sp, trxid);
		checkLength();
	}

	private void checkLength() {
		if (length < 0) {
			return;
		}
		length--;
		if (length == 0) {
			throw new JumpException();
		}
	}

}
