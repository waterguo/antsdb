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

import com.antsdb.saltedfish.cpp.Bytes;
import com.antsdb.saltedfish.cpp.FishSkipList;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.nosql.MemTabletReadOnly.ListNode;
import com.antsdb.saltedfish.util.BytesUtil;
import com.antsdb.saltedfish.util.ConsoleHelper;

/**
 * 
 * @author wgu0
 */
public class MemTabletDumpMain implements ConsoleHelper {

	private File file;
	private MemTabletReadOnly tablet;
	private FishSkipList slist;

	/**
	 * dump the tbl file
	 * 
	 * @throws Exception
	 */
	public static void main(String... args) throws Exception {
		if (args.length != 1) {
			System.err.println("invalid arguments");
			return;
		}
		File file = new File(args[0]);
		if (!file.exists()) {
			System.err.println("file not found");
			return;
		}
		MemTabletDumpMain main = new MemTabletDumpMain();
		main.file = file;
		main.tablet = new MemTabletReadOnly(file);;
		main.slist = main.tablet.slist;
		main.dump();
	}

	private void dump() {
		println("file: %s", file.toString());
		println("start trx id: %d", tablet.getStartTrxId());
		println("end trx id: %d", tablet.getEndTrxId());
		this.slist.dump();
		int oSkipHead = this.slist.getHead();
		long base = this.tablet.base;
		for (int i=oSkipHead; i!= 0; i=FishSkipList.Node.getNext(base, i)) {
			int oNode = i;
			int oKey = FishSkipList.Node.getKeyOffset(oNode);
			byte[] key = Bytes.get(null, base + oKey);
			println("%08x:%s", oNode, BytesUtil.toHex8(key));
			int oValue = FishSkipList.Node.getValueOffset(oNode);
			if (oValue == 0) {
				continue;
			}
			int oHead = Unsafe.getInt(base + oValue);
			for (ListNode j=ListNode.create(base, oHead); j!=null; j=j.getNextNode()) {
	    		long version = j.getVersion();
	    		long spRow = j.getSpacePointer();
	    		int type = j.getType();
	    		if (type == MemTabletReadOnly.TYPE_INDEX) {
					println("\t%08x [version=%d sprow=%08x type=%02x rowkey=%s]", 
							j.getOffset(), 
							version, 
							spRow, 
							type,
							BytesUtil.toHex8(Bytes.get(null, j.getRowKeyAddress())));
	    		}
	    		else {
					println("\t%08x [version=%d sprow=%08x type=%02x]", j.getOffset(), version, spRow, type);
	    		}
	    	}
		}
	}}
