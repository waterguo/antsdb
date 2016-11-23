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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.cpp.Bytes;
import com.antsdb.saltedfish.cpp.FishObject;
import com.antsdb.saltedfish.cpp.FishSkipList;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.cpp.SkipListScanner;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.nosql.GTableReadOnly;
import com.antsdb.saltedfish.nosql.HumpbackReadOnly;
import com.antsdb.saltedfish.nosql.MemTablet;
import com.antsdb.saltedfish.nosql.MemTabletReadOnly;
import com.antsdb.saltedfish.nosql.MemTabletReadOnly.ListNode;
import com.antsdb.saltedfish.util.BytesUtil;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.sql.meta.TableId;
import com.antsdb.saltedfish.sql.meta.RuleMeta.Rule;
import com.antsdb.saltedfish.sql.vdm.SysRuleRow;

/**
 * 
 * @author wgu0
 */
public class FishFindMain extends FishCommandLine {
	public static void main(String[] args) throws Exception {
		new FishFindMain(args).run();
	}

	private HumpbackReadOnly humpback;
	private int tableId;
	private boolean isIndex = false;

	protected Options getOptions() {
		Options options = new Options();
		options.addOption("h", "help", false, "print help");
		options.addOption("t", "table", true, "table id");
		options.addOption("r", "row", true, "find row by HEX");
		options.addOption("x", "rindex", true, "find index by row key");
		options.addOption(null, "index", false, "talbe is an index");
		return options;
	}
	
	public FishFindMain(String[] args) throws ParseException {
		super(args);
	}
	
	private void run() throws Exception {
		if (cmd.getOptions().length == 0 || cmd.hasOption('h')) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("antsdb-find", getOptions());
			return;
		}

		if (cmd.hasOption("t")) {
			this.tableId = Integer.parseInt(cmd.getOptionValue("t"));
		}
		
		this.humpback = getHumpbackReadOnly();
		this.isIndex  = cmd.hasOption("index");
		if (cmd.hasOption('r')) {
			findRow(cmd.getOptionValue('r'));
			return;
		}
		else if (cmd.hasOption('x')) {
			findIndex(cmd.getOptionValue('x'));
			return;
		}
	}

	private void findIndex(String value) {
		if (value.indexOf('*') >= 0) {
			findAllIndex(value);
		}
		else {
			findSingleIndex(value);
		}
	}
	
	private void findAllIndex(String value) {
		/*
		byte[] bytes = BytesUtil.hexToBytes(value);
		long pKey = Unsafe.allocateMemory(1024);
		*/
	}

	private void findSingleIndex(String value) {
		byte[] bytes = BytesUtil.hexToBytes(value);
		long pKey = Unsafe.allocateMemory(1024);
		Bytes.set(pKey, bytes);
		List<GTableReadOnly> indexes = getIndexes();
		for (GTableReadOnly i:indexes) {
			for (MemTabletReadOnly j:i.getMemTable().getTabletsReadOnly()) {
				FishSkipList slist = j.getSkipList();
				long base = j.getBaseAddress();
				for (SkipListScanner k=slist.scan(0, true, 0, true);k.next();) {
					long pHead = k.getValuePointer();
					int oHead = Unsafe.getInt(pHead);
					for (ListNode l=ListNode.create(base, oHead); l!=null; l=l.getNextNode()) {
						if (!equals(pKey, l.getRowKeyAddress())) {
							continue;
						}
						printIndex(j, k.getKeyPointer(), l);
					}
				}
			}
		}
	}

	private void printIndex(MemTabletReadOnly tablet, long pIndexKey, ListNode node) {
		byte[] bytes = Bytes.get(null, pIndexKey);
		println(BytesUtil.toCompactHex(bytes));
		println("    %s:%08x", tablet.getFile(), node.getOffset() + MemTablet.HEADER_SIZE);
		println(node.toString());
	}

	private boolean equals(long px, long py) {
		int lenx = Bytes.getLength(px);
		int leny = Bytes.getLength(py);
		if (lenx != leny) {
			return false;
		}
		for (int i=0; i<lenx; i++) {
			int x = Unsafe.getByte(px + 4 + i);
			int y = Unsafe.getByte(py + 4 +i);
			if (x != y) {
				return false;
			}
		}
		return true;
	}

	private List<GTableReadOnly> getIndexes() {
		List<GTableReadOnly> list = new ArrayList<>();
		GTableReadOnly table = this.humpback.getTable(-TableId.SYSRULE.ordinal());
		for (RowIterator i=table.scan(0, Long.MAX_VALUE);;) {
			if (!i.next()) {
				break;
			}
			SysRuleRow row = new SysRuleRow(i.getRow());
			if (row.getRuleType() != Rule.Index.ordinal()) {
				continue;
			}
			if (row.getTableId() == this.tableId) {
				GTableReadOnly index = this.humpback.getTable(row.getIndexTableId());
				if (index == null) {
					println("error: index not found for %d", row.getIndexTableId());
					continue;
				}
				list.add(index);
			}
		}
		return list;
	}

	private void findRow(String value) {
		GTableReadOnly table = this.humpback.getTable(this.tableId);
		if (table == null) {
			println("error: table %d is not found", this.tableId);
			return;
		}
		if (value.indexOf('*') >= 0) {
			findAllRow(table, value);
		}
		else {
			findSingleRow(table, value);
		}
	}

	private void findAllRow(GTableReadOnly table, String value) {
		RowIterator it = table.scan(0, Long.MAX_VALUE);
		while (it.next()) {
			long pKey = it.getKeyPointer();
			if (this.isIndex) {
				printIndex(it);
			}
			else {
				long spRow = table.get(0, Long.MAX_VALUE, pKey);
				Row row = it.getRow();
				printRow(table, row, spRow);
				printRowHistory(table, row.getKeyAddress());
			}
		}
	}

	private void findSingleRow(GTableReadOnly table, String value) {
		byte[] key = BytesUtil.hexToBytes(value);
		try (BluntHeap heap = new BluntHeap()){
			long pKey = KeyBytes.allocSet(heap, key).getAddress();
			Row row = table.getRow(0, Long.MAX_VALUE, pKey);
			long spRow = table.get(0, Long.MAX_VALUE, pKey);
			if (spRow != 0) {
				printRow(table, row, spRow);
			}
			else {
		        println("%s: row is not found", BytesUtil.toHex8(key));
			}
			printRowHistory(table, pKey);
		}
	}
	
	private void printIndex(RowIterator it) {
		long pIndexKey = it.getKeyPointer();
		long pRowKey = it.getRowKeyPointer();
		String indexKey = BytesUtil.toHex8(KeyBytes.get(pIndexKey));
		String rowKey = BytesUtil.toHex8(KeyBytes.get(pRowKey));
		println("%s", indexKey);
		println("  rowkey:%s", rowKey);
		println("  misc:%d", it.getMisc() & 0xff);
	}
	
	private void printRow(GTableReadOnly table, Row row, long spRow) {
		println("%s", BytesUtil.toHex8(row.getKey()));
		println("  columns");
		for (int i=0; i<=row.getMaxColumnId(); i++) {
			String text = getColumnText(row, i);
			println("    %d:%s", i, text);
		}
	}
	
	private void printRowHistory(GTableReadOnly table, long pKey) {
		println("  reversions");
		for (MemTabletReadOnly i:table.getMemTable().getTabletsReadOnly()) {
			List<ListNode> list = i.getVersions(0, Long.MAX_VALUE, pKey);
			if (list == null) {
				break;
			}
			for (ListNode j:list) {
				println("    %s:%08x %s", i.getFile(), j.getOffset() + MemTablet.HEADER_SIZE, j.toString());
			}
		}
	}

	private String getColumnText(Row row, int i) {
		String text = stringOf(row.getFieldAddress(i));
		if ((i >=0) && (text.length() > 75)) {
			text = text.substring(0, 75) + " ...";
		}
		if (row.getFieldAddress(i) != 0) {
			int type = Value.getFormat(null, row.getFieldAddress(i)) & 0xff;
			text = String.format("[%02x]%s", type, text);
		}
		return text;
	}
	
	private String stringOf(long pValue) {
		int format = Value.getFormat(null, pValue);
		if (format == Value.FORMAT_BYTES) {
			byte[] bytes = Bytes.get(null, pValue);
			return BytesUtil.toCompactHex(bytes);
		}
		String text = String.valueOf(FishObject.get(null, pValue));
		return text;
	}
}
