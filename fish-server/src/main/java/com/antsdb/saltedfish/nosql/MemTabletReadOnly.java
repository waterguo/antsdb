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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.function.IntPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.cpp.FishSkipList;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.cpp.SkipListScanner;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.cpp.VariableLengthLongComparator;
import com.antsdb.saltedfish.storage.HBaseStorageService;
import com.antsdb.saltedfish.util.BytesUtil;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author wgu0
 */
public class MemTabletReadOnly implements Closeable {
	static Logger _log = UberUtil.getThisLogger();
    static Pattern _ptn = Pattern.compile("^([0-9a-fA-F]{8})-([0-9a-fA-F]{8})\\.tbl$");
    static final VariableLengthLongComparator _comp = new VariableLengthLongComparator();

	final static long SIG = 0x73746e61;
	final static byte VERSION = 0;
	public final static int HEADER_SIZE = 0x100;
	final static int OFFSET_SIG = 0;
	final static int OFFSET_CARBONIZED = OFFSET_SIG + 8;
	final static int OFFSET_VERSION = 0x10;
	//final static int OFFSET_START_TRXID = 0x20;
	//final static int OFFSET_END_TRXID = 0x28;
	final static int OFFSET_START_ROW = 0x30;
	final static int OFFSET_END_ROW = 0x38;
	final static int OFFSET_PARENT = 0x40;
	final static int MARK_ROLLED_BACK = -1;
	final static int MARK_ROW_LOCK = -3;
	
	/** indicate this is a tomb stone */
	protected final static byte TYPE_ROW = 0;
	/** indicate this is a tomb stone */
	protected final static byte TYPE_DELETE = 1;
	/** indicate this is a locked records */
	protected final static byte TYPE_LOCK = 2;
	/** indicate this is a locked records */
	protected final static byte TYPE_INDEX = 3;
	
	FishSkipList slist;
	long base;
	TrxMan trxman;
	File file;
	MemoryMappedFile mmap;
	AtomicLong startTrx = new AtomicLong(Long.MIN_VALUE);
	AtomicLong endTrx = new AtomicLong(0);
	int tableId;
	int tabletId;

	public final static class ListNode {
		private final static int OFFSET_TRX_VERSION = 0;
		private final static int OFFSET_NEXT = 0x8; 
		private final static int OFFSET_TYPE = 0xc; 
		private final static int OFFSET_MISC = 0xd; 
		private final static int OFFSET_SPACE_POINTER = 0xe;
		private final static int OFFSET_ROW_KEY = 0x16;
		private final static int OFFSET_END = 0x16;
		
		private long base;
		private int offset;

		public ListNode(long base, int offset) {
			this.base = base;
			this.offset = offset;
		}
		
		public long getAddress() {
			return this.base + this.offset;
		}
		
		public static ListNode create(long base, int offset) {
			if (offset == 0) {
				return null;
			}
			return new ListNode(base, offset);
		}
		
		static ListNode alloc(BluntHeap heap, long version, long value, int next) {
			int result = heap.allocOffset(OFFSET_END);
			ListNode node = new ListNode(heap.getAddress(0), result);
			node.setVersion(version);
			node.setSpacePointer(value);
			node.setNext(next);
			heap.putByteVolatile(node.offset + OFFSET_TYPE, TYPE_ROW);
			return node;
		}
		
		public static ListNode allocIndex(BluntHeap heap, long sp, long version, long pRowKey, int next) {
			int keySize = (pRowKey != 0) ? KeyBytes.getRawSize(pRowKey) : 0;
			int offset = heap.allocOffset(OFFSET_END + keySize);
			ListNode node = new ListNode(heap.getAddress(0), offset);
			node.setVersion(version);
			node.setSpacePointer(sp);
			node.setNext(next);
			heap.putByteVolatile(node.offset + OFFSET_TYPE, TYPE_INDEX);
			Unsafe.copyMemory(pRowKey, heap.getAddress(node.getOffset() + OFFSET_ROW_KEY), keySize);
			return node;
		}

		static long getRowKeyAddress(long base, int offset) {
			return base + offset + OFFSET_ROW_KEY;
		}
		
		public long getRowKeyAddress() {
			return this.base + this.offset + OFFSET_ROW_KEY;
		}
		
		long getVersion() {
			return Unsafe.getLongVolatile(this.base + this.offset + OFFSET_TRX_VERSION);
		}
		
		void setVersion(long trxts) {
			Unsafe.putLongVolatile(this.base + this.offset + OFFSET_TRX_VERSION, trxts);
		}
		
		long getSpacePointer() {
			return Unsafe.getLongVolatile(this.base + this.offset + OFFSET_SPACE_POINTER);
		}
		
		void setSpacePointer(long value) {
			Unsafe.putLongVolatile(this.base + this.offset + OFFSET_SPACE_POINTER, value);
		}
		
		int getNext() {
			return Unsafe.getIntVolatile(this.base + this.offset + OFFSET_NEXT);
		}

		void setNext(int value) {
			Unsafe.putIntVolatile(this.base + this.offset + OFFSET_NEXT, value);
		}
		
		public ListNode getNextNode() {
			int next = getNext();
			ListNode node = create(this.base, next);
			return node;
		}
		
		boolean casNext(int oldValue, int newValue) {
			return Unsafe.compareAndSwapInt(this.base + this.offset + OFFSET_NEXT, oldValue, newValue);
		}

		public int getOffset() {
			return this.offset;
		}

		boolean isDeleted() {
			return getType() == TYPE_DELETE;
		}
		
		void setDeleted(boolean b) {
			Unsafe.putByteVolatile(this.base + this.offset + OFFSET_TYPE, b ? TYPE_DELETE : TYPE_ROW);
		}

		boolean isLocked() {
			return getType() == TYPE_LOCK;
		}

		void setLocked(boolean b) {
			Unsafe.putByteVolatile(this.base + this.offset + OFFSET_TYPE, TYPE_LOCK);
		}
		
		public boolean isRow() {
			return getType() == TYPE_ROW;
		}
		
		byte getType() {
			byte type = Unsafe.getByteVolatile(this.base + this.offset + OFFSET_TYPE);
			return type;
		}

		byte getMisc() {
			byte misc = Unsafe.getByteVolatile(this.base + this.offset + OFFSET_MISC);
			return misc;
		}
		
		void setMisc(byte value) {
			Unsafe.putByteVolatile(this.base + this.offset + OFFSET_MISC, value);
		}
		
		@Override
		public String toString() {
			String str = String.format(
				"    %08x [version=%d, sprow=%08x, type=%02x]", 
				this.offset, 
				getVersion(), 
				getSpacePointer(), 
				getType());
			return str;
		}

		public int copy(BluntHeap heap) {
			int size = OFFSET_END;
			if (getType() == TYPE_INDEX) {
				size = size + KeyBytes.getRawSize(getRowKeyAddress());
			}
			int offset = heap.allocOffset(OFFSET_END + size);
			long pTarget = heap.getAddress(offset);
			Unsafe.copyMemory(getAddress(), pTarget, size);
			ListNode copy = new ListNode(heap.getAddress(0), offset);
			copy.setNext(0);
			return offset;
		}
	}
	
	static class Scanner {
		MemTabletReadOnly tablet;
		SkipListScanner upstream;
		long version;
		long trxid;
		long base;
		TrxMan trxman;
		private int oNext;
		private long spNextRow;
		private int type;
		
		long getVersion() {
			ListNode node = new ListNode(base, oNext);
			return node.getVersion();
		}
		
		long getRowPointer() {
			return this.spNextRow;
		}

		long getKeyPointer() {
			return this.upstream.getKeyPointer(); 
		}
		
		public long getRowKeyPointer() {
			return ListNode.getRowKeyAddress(base, oNext);
		}
		
		Row getRow(SpaceManager memman) {
			if (this.spNextRow == 0) {
				return null;
			}
			Row row = Row.fromSpacePointer(memman, this.spNextRow, version);
			return row;
		}
		
		void rewind() {
			this.upstream.rewind();
			this.oNext = 0;
		}
		
		boolean eof() {
			return this.upstream.eof();
		}
		
		public boolean next() {
			for (;;) { 
				if (!this.upstream.next()) {
					this.oNext = 0;
					return false;
				}
				long pHead = this.upstream.getValuePointer();
				int oNode = getVersionNode(this.base, trxman, pHead, this.trxid, this.version);
				if (oNode != 0) {
					ListNode node = new ListNode(this.base, oNode);
					if (!node.isDeleted()) {
						this.spNextRow = node.getSpacePointer();
					}
					else {
						this.spNextRow = 1;
					}
					this.oNext = oNode;
					this.type = node.getType();
					return true;
				}
			}
		}

		public boolean isRow() {
			return this.type == TYPE_ROW;
		}

		public long getIndexSuffix() {
			long pKey = getKeyPointer();
			if (pKey == 0) {
				return 0;
			}
			long suffix = KeyBytes.create(pKey).getSuffix();
			return suffix;
		}

		public byte getMisc() {
			ListNode node = new ListNode(base, oNext);
			return node.getMisc();
		}

	}
	
	public MemTabletReadOnly(File file) throws IOException {
		this.file = file;
		this.tableId = getTableId(file);
		this.tabletId = getTabletId(file);
		if (this.file.length() <= (HEADER_SIZE + 8)) {
			throw new IOException("file " + file + " has incorrect length");
		}
		this.mmap = new MemoryMappedFile(file, "r");
		this.base = this.mmap.getAddress();
		this.slist = new FishSkipList(this.base, HEADER_SIZE, _comp);
		this.tableId = getTableId();
		if (getSig() != SIG) {
			throw new IOException("SIG is not found: " + file);
		}
		if (getVersion() != VERSION) {
			throw new IOException("incorrect version is found: " + file);
		}
	}

	protected MemTabletReadOnly () {
	}
	
	TrxMan getTrxMan() {
		return this.trxman;
	}

	void setTrxMan(TrxMan trxman) {
		this.trxman = trxman;
	}
	
	static int getVersionNode(long base, TrxMan trxman, long pHead, long trxid, long version) {
		int head = Unsafe.getInt(pHead);
		if (head == 0) {
			return 0;
		}
		for (ListNode i=ListNode.create(base, head); i!=null; i=i.getNextNode()) {
    		long versionInList = i.getVersion();
			
			// first trying to matched version

    		if (versionInList < 0) {
        		if (trxid != versionInList) {
        			if (trxman != null) {
        				versionInList = trxman.getTimestamp(versionInList);
        			}

        			// found a rolled back version, skip
        			if (versionInList == MARK_ROLLED_BACK) {
        				continue;
        			}
        			// found a pending update by another trx, skip
        			if (versionInList < 0) {
        				continue;
        			}
        		}
    		}
			if (version < versionInList) {
				// this version is too new, skip
				continue;
			}

    		// found a match. make sure this is not row lock
    		
    		if (i.isLocked()) {
    			continue;
    		}
    		
    		// finally found a qualified version 
    		
			return i.getOffset();
    	}
    	return 0;
	}

	/**
	 * 
	 * @param pKey
	 * @param trxid
	 * @param version
	 * @param missing
	 * @return space pointer to the row. 0 means not found. 1 means tomb stone
	 */
	public long get(long trxid, long version, long pKey, long missing) {
		long pHead = this.slist.get(pKey);
		if (pHead == 0) {
			return 0;
		}
		int versionNode = getVersionNode(this.base, getTrxMan(), pHead, trxid, version);
		if (versionNode == 0) {
			return missing;
		}
		ListNode node = new ListNode(this.base, versionNode);
		return (node.isDeleted()) ? 1 : node.getSpacePointer();
	}
	
	public List<ListNode> getVersions(long trxid, long version, long pKey) {
		List<ListNode> list = new ArrayList<>();
		long pHead = this.slist.get(pKey);
		if (pHead == 0) {
			return list;
		}
		int oHead = Unsafe.getInt(pHead);
		for (ListNode i=ListNode.create(this.base, oHead); i!=null; i=i.getNextNode()) {
			list.add(i);
    	}
		return list;
	}
	
	public int getRow(RowKeeper keeper, long trxid, long version, long pKey) {
		ListNode node = getListNode(trxid, version, pKey);
		if (node == null) {
			return 0;
		}
		if (!node.isDeleted()) {
			long spRow = node.getSpacePointer();
			long rowVersion = node.getVersion();
			keeper.sprow = spRow;
			keeper.version = rowVersion;
			return 1;
		}
		else {
			return -1;
		}
	}
	
	public long getIndex(long trxid, long version, long pKey) {
		ListNode node = getListNode(trxid, version, pKey);
		if (node == null) {
			return 0;
		}
		return  (!node.isDeleted()) ? node.getRowKeyAddress() : 1;
	}
	
	private ListNode getListNode(long trxid, long version, long pKey) {
		long pHead = this.slist.get(pKey);
		if (pHead == 0) {
			return null;
		}
		int oVersion = getVersionNode(this.base, getTrxMan(), pHead, trxid, version);
		if (oVersion == 0) {
			return null;
		}
		ListNode node = new ListNode(this.base, oVersion);
		return node;
	}
	
	Scanner scan(
			long trxid, 
			long version, 
			long pKeyStart, 
			boolean includeStart, 
			long pKeyEnd, 
			boolean includeEnd,
			boolean isAscending) {
		SkipListScanner upstream = null; 
		if (isAscending) {
			upstream = this.slist.scan(pKeyStart, includeStart, pKeyEnd, includeEnd);
		}
		else {
			upstream = this.slist.scanReverse(pKeyStart, includeStart, pKeyEnd, includeEnd);
		}
		if (upstream == null) {
			return null;
		}
		Scanner scanner = new Scanner();
		scanner.tablet = this;
		scanner.upstream = upstream;
		scanner.base = base;
		scanner.trxman = getTrxMan();
		scanner.version = version;
		scanner.trxid = trxid;
		return scanner;
	}

	void scan(BiPredicate<Long, ListNode> predicate) {
		SkipListScanner scanner = this.slist.scan(0, true, 0, true);
		if (scanner == null) {
			return;
		}
		while (scanner.next()) {
			long pHead = scanner.getValuePointer();
			if (pHead == 0) {
				continue;
			}
			int oHead = Unsafe.getInt(pHead);
			if (oHead == 0) {
				continue;
			}
			ListNode node=ListNode.create(this.base, oHead);
			if (!predicate.test(scanner.getKeyPointer(), node)) {
				break;
			}
		}
	}
	
	void scanAllVersion(IntPredicate predicate) {
		SkipListScanner scanner = this.slist.scan(0, true, 0, true);
		if (scanner == null) {
			return;
		}
		while (scanner.next()) {
			long pHead = scanner.getValuePointer();
			if (pHead == 0) {
				continue;
			}
			int oHead = Unsafe.getInt(pHead);
			if (oHead == 0) {
				continue;
			}
			for (ListNode i=ListNode.create(this.base, oHead); i!=null; i=i.getNextNode()) {
	    		boolean cont = predicate.test(i.getOffset());
	    		if (!cont) {
	    			return;
	    		}
	    	}
		}
	}

	byte getVersion() {
		return Unsafe.getByte(this.mmap.getAddress() + OFFSET_VERSION);
	}
	
	long getSig() {
		return Unsafe.getLong(this.mmap.getAddress() + OFFSET_SIG);
	}

	/**
	 * 
	 * @return Long.MIN_VALUE when there is pending data
	 */
	public long getStartTrxId() {
		long result = this.startTrx.get();
		return result;
	}

	/**
	 * 
	 * @return 0 means there is no data under transaction
	 */
	public long getEndTrxId() {
		long result = this.endTrx.get();
		return result;
	}
	
	public File getFile() {
		return this.file;
	}
	
	public boolean isPureEmpty() {
		return this.slist.isEmpty();
	}

	public FishSkipList getSkipList() {
		return this.slist;
	}

	int size() {
		return this.slist.size();
	}

	boolean validate() {
		_log.debug("validating {} ...", this.file);
		AtomicBoolean result = new AtomicBoolean(true);
		long now = getTrxMan().getNewVersion();
		scanAllVersion(oVersion -> {
			ListNode node = new ListNode(this.base, oVersion);
    		long version = node.getVersion();
    		long value = node.getSpacePointer();
    		int next = node.getNext();
    		if (version < -10) {
    			_log.error("negtive version {}:{} [version={} value={} next={}]",
    					   this.file.getName(),
    					   oVersion, 
    					   version, 
    					   value, 
    					   next);
    			result.set(false);
    		}
    		if (version > 0) {
    			if (version > now) {
	    			_log.error("version is larger than it is supposed to be {} [version={} value={} next={}]",
	    					   this.file.getName(),
	    					   oVersion, 
	    					   version, 
	    					   value, 
	    					   next);
	    			result.set(false);
    			}
    		}
    		return true;
		});
		return result.get();
	}
	
	String dumpVersions(long pHead) {
		StringBuilder buf = new StringBuilder();
		buf.append(pHead);
		buf.append(" ");
		int head = Unsafe.getInt(pHead);
		buf.append(head);
		buf.append("\n");
		if (head != 0) {
			for (ListNode i=ListNode.create(this.base, head); i!=null; i=i.getNextNode()) {
	    		long version = i.getVersion();
	    		long value = i.getSpacePointer();
	    		int next = i.getNext();
	    		buf.append(i);
	    		buf.append("[version=");
	    		buf.append(version);
	    		buf.append(" value=");
	    		buf.append(value);
	    		buf.append(" next=");
	    		buf.append(next);
	    		buf.append("]\n");
	    	}
		}
		return buf.toString();
	}

	String dumpKey(long pKey) {
		byte[] key = KeyBytes.create(pKey).get();
		return BytesUtil.toHex(key);
	}

	@Override
	public synchronized void close() {
		if (isClosed()) {
			return;
		}
		this.mmap.unmap();
		this.mmap = null;
		this.slist = null;
		this.base = 0;
	}

	public boolean isClosed() {
		return this.mmap == null;
	}
	
	public long getEndRow() {
		long result = Unsafe.getLong(this.mmap.getAddress() + OFFSET_END_ROW);
		return result;
	}
	
	protected int getRowStateForWrite(
			long pKey, 
			int oHead, 
			long trxid, 
			long trxts, 
			Collection<MemTabletReadOnly> pastTablets) {
		int state = getRowStateForWrite(oHead, trxid, trxts);
		if (state != RowState.NONEXIST) {
			return state;
		}
		state = getRowStateForWrite(pKey, trxid, trxts, pastTablets);
		return state;
	}
	
	private int getRowStateForWrite(
			long pKey, 
			long trxid, 
			long compare, 
			Collection<MemTabletReadOnly> pastTablets) {
		// check history tablets
		
		for (MemTabletReadOnly i:pastTablets) {
			if (i == this) {
				continue;
			}
			long pHead = i.slist.get(pKey);
			if (pHead == 0) {
				continue;
			}
			int oHead = Unsafe.getInt(pHead);
			int rowState = i.getRowStateForWrite(oHead, trxid, compare);
			if (rowState != RowState.NONEXIST) {
				return rowState;
			}
		}
		
		// check hbase
		
		HBaseStorageService hbase = getHbase();
		if (hbase == null) {
			return RowState.NONEXIST;
		}
		boolean exists = hbase.exists(tableId, pKey);
		return exists ? RowState.EXIST : RowState.NONEXIST;
	}
	
	private HBaseStorageService getHbase() {
		return null;
	}

	private int getRowStateForWrite(int oHead, long trxid, long compare) {
		for (ListNode i=ListNode.create(this.base, oHead); i!=null; i=i.getNextNode()) {
			long currentRawVersion;
			long currentVersion;
			currentRawVersion = i.getVersion();
			
			// realize the version if this is a trxid
			
			if (currentRawVersion < -10) {
				currentVersion = getTrxMan().getTimestamp(currentRawVersion);
			}
			else {
				currentVersion = currentRawVersion;
			}
			
			// skip if it is rolled back
			
			if (currentVersion == -1) {
				// rolled back updates, skip
				currentVersion = 0;
				continue;
			}
			
			// lock detection
			
			if ((trxid < 0) && (currentVersion < 0)) {
				if (trxid != currentVersion) {
					// row is locked by another trx
					return HumpbackError.LOCK_COMPETITION.ordinal();
				}
			}
			
			// compare and update check
			
			if (compare != 0) {
				// expired lock record. skip because it doesn't tell the state. we need the node with data for the 
				// subsequent checks. keep in mind, a key can't be locked without data.
				
				if (i.getType() == TYPE_LOCK) {
					continue;
				}
				
				if ((compare != currentVersion) && (compare != currentRawVersion) && (trxid != currentVersion)) {
					return HumpbackError.CONCURRENT_UPDATE.ordinal();
				}				
			}
			
			// found it
			
			if (!i.isDeleted()) {
				return (currentVersion != trxid) ? RowState.EXIST : RowState.EXIST_AND_LOCKED;
			} 
			else {
				return RowState.TOMBSTONE;
			}
		}
		
		return RowState.NONEXIST;
	}
	
	public int getTabletId() {
		return this.tabletId;
	}
	
	int getTableId() {
		return this.tableId;
	}
	
	static int getTableId(File file) {
        Matcher m = _ptn.matcher(file.getName());
        if (!m.find()) {
        	return -1;
        }
        int tableId = Long.decode("0x" + m.group(1)).intValue();
        return tableId;
	}

	static int getTabletId(File file) {
        Matcher m = _ptn.matcher(file.getName());
        if (!m.find()) {
        	return -1;
        }
        int tabletId = Long.decode("0x" + m.group(2)).intValue();
        return tabletId;
	}

	@Override
	public String toString() {
		return this.file.toString();
	}

	public synchronized void drop() {
		if (this.file == null) {
			return;
		}
		close();
		_log.debug("deleting file {} ...", this.file);
		this.file.delete();
	}

	public long getBaseAddress() {
		return this.base;
	}
	
}
