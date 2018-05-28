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
package com.antsdb.saltedfish.nosql;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.cpp.FishSkipList;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.cpp.VariableLengthLongComparator;
import com.antsdb.saltedfish.nosql.MemTablet.ListNode;
import com.antsdb.saltedfish.util.BytesUtil;
import com.antsdb.saltedfish.util.CommandLineHelper;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author wgu0
 */
public class DumpRow extends CommandLineHelper {
	int tableId;
	byte[] key;
	long pKey;
	File home;
	List<File> files = new ArrayList<>();
	List<Tablet> tablets = new ArrayList<>();
	boolean isVerbose;
	BluntHeap heap = new BluntHeap();
	private SpaceManager spaceman;
	private boolean dumpHex;
	private boolean dumpValues;

	private class Tablet {
		File file;
		MappedByteBuffer buf;
	}
	
	public static void main(String[] args) throws Exception {
		new DumpRow().run(args);
	}

	private Options getOptions() {
		Options options = new Options();
		options.addOption("h", "help", false, "print help");
		options.addOption(null, "home", true, "data directory");
		options.addOption("v", null, false, "verbose");
		options.addOption(null, "hex", false, "hex dump of the row");
		options.addOption(null, "values", false, "value dump of the row");
		return options;
	}

	private void run(String[] args) throws Exception {
		CommandLine line = parse(getOptions(), args);

		// help
		
		if (line.hasOption("help")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("dumprow <table id> <row key base64>", getOptions());
			return;
		}
		
		// arguments
		
		if (line.getArgList().size() != 2) {
			println("error: invalid command line arguments");
			return;
		}
		this.tableId = Integer.parseInt(line.getArgList().get(0));
		this.key = Base64.getDecoder().decode(line.getArgList().get(1));
		this.pKey = KeyBytes.allocSet(this.heap, this.key).getAddress();
		
		// options
		
		this.home = new File(line.getOptionValue("home"));
		this.isVerbose = line.hasOption('v');
		this.dumpHex = line.hasOption("hex");
		this.dumpValues = line.hasOption("values");
		
		// validate
		
		if (!this.home.isDirectory()) {
			println("error: invalid home directory {}", this.home);
			return;
		}
		if (!new File(this.home, "checkpoint.bin").exists()) {
			println("error: invalid home directory {}", this.home);
			return;
		}
		
		// init space manager
		
		this.spaceman = new SpaceManager(this.home, false);
		spaceman.open();
		
		// proceed
		
		dump();
	}

	private void findTabletFiles(File dir) {
		for (File i:dir.listFiles()) {
			if (i.isDirectory()) {
				findTabletFiles(i);
				continue;
			}
			if (!i.getName().endsWith(".tbl")) {
				continue;
			}
			if (MemTablet.getTableId(i) == this.tableId) {
				this.files.add(i);
			}
		}
	}
	
	private void dump() throws Exception {
		if (this.isVerbose) {
			String str = BytesUtil.toHex(this.key);
			str = StringUtils.replace(str, "\n", "\n     ");
			println("key: %s", str);
		}
		
		// find tablet files
		
		this.findTabletFiles(this.home);
		Collections.sort(this.files);
		Collections.reverse(this.files);
		
		// create mapped buffers
		
		for (File i:this.files) {
			try (RandomAccessFile raf = new RandomAccessFile(i, "r")) {
				Tablet tablet = new Tablet();
				this.tablets.add(tablet);
				tablet.buf = raf.getChannel().map(MapMode.READ_ONLY, 0, i.length());;
				tablet.file = i;
			}
			if (isVerbose) {
				println("loading file: %s", i);
			}
		}
		
		// find row
		
		List<Long> sprows = new ArrayList<>();
		for (Tablet tablet:this.tablets) {
			MappedByteBuffer buf = tablet.buf;
			long addr = UberUtil.getAddress(buf);
			BluntHeap heap = new BluntHeap(addr + MemTablet.HEADER_SIZE, 0);
			heap.position(buf.capacity() - MemTablet.HEADER_SIZE);
			FishSkipList slist = FishSkipList.alloc(heap, new VariableLengthLongComparator());
			long pHead = slist.get(this.pKey);
			if (pHead == 0) {
				continue;
			}
			dumpRowVersions(tablet, heap, pHead, sprows);
		}
		
		// dump each row
		
		for (long spRow:sprows) {
			dumpRow(spRow);
		}
	}

	private void dumpRow(long spRow) throws Exception {
		println("row:%x %s", spRow, this.spaceman.getLocation(spRow));
		long pRow = this.spaceman.toMemory(spRow);
		Row row = Row.fromMemoryPointer(pRow, 0);
		if (this.dumpValues) {
			println("%s", row.toString());
		}
		if (this.dumpHex) {
			int len = row.getLength();
			println("%s", BytesUtil.toHex(pRow, len));
		}
	}

	private void dumpRowVersions(Tablet tablet, BluntHeap heap, long pHead, List<Long> sprows) {
		int oHead = Unsafe.getInt(pHead);
		println("revision list found at %s@%x", tablet.file, heap.getAddress(oHead) + MemTablet.HEADER_SIZE);
		for (ListNode i=ListNode.create(heap.getAddress(0), oHead); i!=null; i=i.getNextNode()) {
			long version = i.getVersion();
			long spRow = i.getSpacePointer();
			sprows.add(spRow);
			println("@%x version=%x sprow=%x", i.getOffset() + MemTablet.HEADER_SIZE, version, spRow);
		}
	}

}
