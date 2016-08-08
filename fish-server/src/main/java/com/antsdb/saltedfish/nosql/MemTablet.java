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
import java.io.RandomAccessFile;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.cpp.Bytes;
import com.antsdb.saltedfish.cpp.FishSkipList;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.util.AtomicUtil;
import com.antsdb.saltedfish.util.BytesUtil;
import com.antsdb.saltedfish.util.CodingError;
import com.antsdb.saltedfish.util.ConsoleHelper;
import com.antsdb.saltedfish.util.UberTimer;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * a tablet is a wrapper of skiplist and a composing piece of memtable
 *  
 * @author wgu0
 */
public final class MemTablet extends MemTabletReadOnly implements ConsoleHelper {
	static Logger _log = UberUtil.getThisLogger();
    static int _alignment = 1;

	BluntHeap heap;
	int size;
	Humpback humpback;
	private AtomicInteger gate = new AtomicInteger();
	private AtomicLong casRetries = new AtomicLong();
	private AtomicLong lockwaits = new AtomicLong();
	private AtomicLong errorConcurrencies = new AtomicLong();
	
	static {
		String osarch = System.getProperty("os.arch");
		if ("ppc64le".equals(osarch)) {
			_log.info("ppc64le is detected. heap alignment is set to 4");
			// ppc64le CAS instruction requires 4 bytes aligned
			_alignment = 4;
		}
	}
	
	MemTablet(Humpback humpback, File ns, int tableId, int tabletId, int size) throws IOException {
		this(ns, tableId, tabletId, size);
		this.humpback = humpback;
		setTrxMan(humpback.trxMan);
	}
	
	public MemTablet(Humpback humpback, File file) throws IOException {
		super(file);
		this.humpback = humpback;
		setTrxMan(humpback.trxMan);
		this.heap = new BluntHeap(this.base, 0);
		this.heap.setAlignment(_alignment);
		this.heap.freeze();
 	}

	/**
	 * construct a writable instance 
	 * @param ns
	 * @param tableId
	 * @param tabletId
	 * @param baseTabletId
	 * @param size
	 * @throws IOException
	 */
	private MemTablet(File ns, int tableId, int tabletId, int size) 
	throws IOException {
		this.tableId = tableId;
		this.tabletId = tabletId;
		this.file = getFile(ns, tableId, tabletId);
		this.size = size;
		
		// creating new file
		
		_log.debug("creating new tablet {} ...", file);
		this.mmap = new MemoryMappedFile(file, size, "rw");
		this.base = this.mmap.getAddress();
		this.heap = new BluntHeap(this.base, size);
		this.heap.setAlignment(_alignment);
		this.heap.alloc(HEADER_SIZE);
		initFile();
		
		// init variables
		
		setCarbonized(false);
		setStartRow(Long.MAX_VALUE);
		this.slist = FishSkipList.alloc(heap, _comp);
	}
	
	static File getFile(File ns, int tableId, int tabletId) {
		File file = new File(ns, getFileName(tableId, tabletId));
		return file;
	}

    static String getFileName(int tableId, int tabletId) {
        String name = String.format("%08x-%08x.tbl", tableId, tabletId);
        return name;
    }
    
	private void initFile() {
		setSig(SIG);
		setVersion(VERSION);
		setStartRow(Long.MAX_VALUE);
		setEndRow(0);
	}
	
	/**
	 * 
	 * @param version
	 * @param pKey
	 * @param row null if this is mark-delete
	 * @return
	 */
	HumpbackError put(VaporizingRow row, Collection<MemTabletReadOnly> past) {
		try {
			this.gate.incrementAndGet();
			long version = row.getVersion();
			long pKey = row.getKeyAddress();
			for (;;) {
				long pHead = this.slist.put(pKey);
				int oHeadValue = Unsafe.getIntVolatile(pHead);
				int rowState = getRowStateForWrite(pKey, oHeadValue, version, 0, past);
				HumpbackError error = check(rowState, false, false);
				if (error == HumpbackError.SUCCESS) {
					ListNode node = alloc(version, 0, oHeadValue);
					if (row != null) {
						long spRow = this.humpback.getGobbler().logRow(row, this.tableId);
						node.setSpacePointer(spRow);
					}
					else {
						node.setDeleted(true);
					}
					if (!casHead(pHead, oHeadValue, node.getOffset())) {
						this.casRetries.incrementAndGet();
						continue;
					}
					trackTrxId(version, node.getOffset());
					return HumpbackError.SUCCESS;
				}
				return error;
			}
		}
		finally {
			this.gate.decrementAndGet();
		}
	}
	
	HumpbackError putNoLogging(long pKey, long version, long value, Collection<MemTabletReadOnly> past) {
		try {
			this.gate.incrementAndGet();
			for (;;) {
				long pHead = this.slist.put(pKey);
				int oHeadValue = Unsafe.getIntVolatile(pHead);
				int rowState = getRowStateForWrite(pKey, oHeadValue, version, 0, past);
				HumpbackError error = check(rowState, false, false);
				if (error != HumpbackError.SUCCESS) {
					return error;
				}
				ListNode node = alloc(version, value, oHeadValue);
				if (value == 0) {
					node.setDeleted(true);
				}
				if (!casHead(pHead, oHeadValue, node.getOffset())) {
					this.casRetries.incrementAndGet();
					continue;
				}
				trackTrxId(version, node.getOffset());
				return HumpbackError.SUCCESS;
			}
		}
		finally {
			this.gate.decrementAndGet();
		}
	}	
	
	HumpbackError insert(VaporizingRow row, Collection<MemTabletReadOnly> pastTablets, int timeout) {
		try {
			this.gate.incrementAndGet();
			UberTimer timer = new UberTimer(timeout);
			long pKey = row.getKeyAddress();
			long version = row.getVersion();
			for (;;) {
				long pHead = this.slist.put(pKey);
				int oHeadValue = Unsafe.getIntVolatile(pHead);
				int rowState = getRowStateForWrite(pKey, oHeadValue, version, 0, pastTablets);
				HumpbackError error = check(rowState, false, true);
				if (error == HumpbackError.SUCCESS) {
					ListNode node = alloc(version, 0, oHeadValue);
					long spRow = this.humpback.getGobbler().logRow(row, this.tableId);
					node.setSpacePointer(spRow);
					if (!casHead(pHead, oHeadValue, node.getOffset())) {
						this.casRetries.incrementAndGet();
						continue;
					}
					trackTrxId(version, node.getOffset());
					return HumpbackError.SUCCESS;
				}
				if (!lockWait(error, timer, pKey, oHeadValue, version)) {
					return error;
				}
			}
		}
		finally {
			this.gate.decrementAndGet();
		}
	}

	HumpbackError insertIndex(long pIndexKey, 
			long version, 
			long pRowKey, 
			Collection<MemTabletReadOnly> pastTablets, 
			long sp, 
			int timeout) {
		try {
			this.gate.incrementAndGet();
			UberTimer timer = new UberTimer(timeout);
			for (;;) {
				long pHead = this.slist.put(pIndexKey);
				int oHeadValue = Unsafe.getIntVolatile(pHead);
				int rowState = getRowStateForWrite(pIndexKey, oHeadValue, version, 0, pastTablets);
				HumpbackError error = check(rowState, false, true);
				if (error == HumpbackError.SUCCESS) {
					ListNode node = ListNode.allocIndex(heap, sp, version, pRowKey, oHeadValue);
					this.humpback.getGobbler().logIndex(this.tableId, version, pIndexKey, pRowKey);
					if (!casHead(pHead, oHeadValue, node.getOffset())) {
						this.casRetries.incrementAndGet();
						continue;
					}
					trackTrxId(version, node.getOffset());
					return HumpbackError.SUCCESS;
				}
				if (!lockWait(error, timer, pIndexKey, oHeadValue, version)) {
					return error;
				}
			}
		}
		finally {
			this.gate.decrementAndGet();
		}
	}
	
	/**
	 * lock a row 
	 * @param trxid
	 * @param pKey
	 */
	public HumpbackError lock(long trxid, long pKey, Collection<MemTabletReadOnly> pastTablets, int timeout) {
		try {
			this.gate.incrementAndGet();
			UberTimer timer = new UberTimer(timeout);
			for (;;) {
				long pHead = this.slist.put(pKey);
				int oHeadValue = Unsafe.getIntVolatile(pHead);
				int rowState = getRowStateForWrite(pKey, oHeadValue, trxid, 0, pastTablets);
				HumpbackError error = check(rowState, true, false);
				if (error == HumpbackError.SUCCESS) {
					if (rowState == RowState.EXIST_AND_LOCKED) {
						// avoid repeating locking
						return HumpbackError.SUCCESS;
					}
					ListNode node = alloc(trxid, 0, oHeadValue);
					node.setLocked(true);
					if (!casHead(pHead, oHeadValue, node.getOffset())) {
						this.casRetries.incrementAndGet();
						continue;
					}
					trackTrxId(trxid, node.getOffset());
					return HumpbackError.SUCCESS;
				}
				if (!lockWait(error, timer, pKey, oHeadValue, trxid)) {
					return error;
				}
			}
		}
		finally {
			this.gate.decrementAndGet();
		}
	}
	
	HumpbackError update(VaporizingRow row, long compare, Collection<MemTabletReadOnly> pastTablets, int timeout) {
		try {
			this.gate.incrementAndGet();
			UberTimer timer = new UberTimer(timeout);
			long pKey = row.getKeyAddress();
			long version = row.getVersion();
			for (;;) {
				long pHead = this.slist.put(pKey);
				int oHeadValue = Unsafe.getIntVolatile(pHead);
				int rowState = getRowStateForWrite(pKey, oHeadValue, version, compare, pastTablets);
				HumpbackError error = check(rowState, true, false);
				if (error == HumpbackError.SUCCESS) {
					long spRow = this.humpback.getGobbler().logRow(row, this.tableId);
					ListNode node = alloc(version, spRow, oHeadValue);
					if (!casHead(pHead, oHeadValue, node.getOffset())) {
						this.casRetries.incrementAndGet();
						continue;
					}
					trackTrxId(version, node.getOffset());
					return HumpbackError.SUCCESS;
				}
				if (!lockWait(error, timer, pKey, oHeadValue, version)) {
					return error;
				}
			}
		}
		finally {
			this.gate.decrementAndGet();
		}
	}	
	
	HumpbackError delete(long pKey, long compare, Collection<MemTabletReadOnly> pastTablets, long sprow, int timeout) {
		try {
			this.gate.incrementAndGet();
			UberTimer timer = new UberTimer(timeout);
			for (;;) {
				long pHead = this.slist.put(pKey);
				int oHeadValue = Unsafe.getIntVolatile(pHead);
				int rowState = getRowStateForWrite(pKey, oHeadValue, compare, 0, pastTablets);
				HumpbackError error = check(rowState, true, false);
				if (error == HumpbackError.SUCCESS) {
					ListNode node = alloc(compare, sprow, oHeadValue);
					node.setDeleted(true);
					if (!casHead(pHead, oHeadValue, node.getOffset())) {
						this.casRetries.incrementAndGet();
						continue;
					}
					trackTrxId(compare, node.getOffset());
					return HumpbackError.SUCCESS;
				}
				if (!lockWait(error, timer, pKey, oHeadValue, compare)) {
					return error;
				}
			}
		}
		finally {
			this.gate.decrementAndGet();
		}
	}
	
	private HumpbackError check(int rowState, boolean mustExist, boolean mustNotExist) {
		if (rowState == HumpbackError.CONCURRENT_UPDATE.ordinal()) {
			return HumpbackError.CONCURRENT_UPDATE;
		}
		else if (rowState == HumpbackError.LOCK_COMPETITION.ordinal()) {
			return HumpbackError.LOCK_COMPETITION;
		}
		else if (rowState == HumpbackError.VERSION_VIOLATION.ordinal()) {
			return HumpbackError.VERSION_VIOLATION;
		}
		if (mustExist) {
			if ((rowState != RowState.EXIST) && (rowState != RowState.EXIST_AND_LOCKED)) {
				return HumpbackError.EXISTENCE_VIOLATION;
			}
		}
		if (mustNotExist) {
			if ((rowState != RowState.NONEXIST) && (rowState != RowState.TOMBSTONE)) {
				return HumpbackError.EXISTENCE_VIOLATION;
			}
		}
		return HumpbackError.SUCCESS;
	}
	
	private boolean casHead(long pHead, int oOldValue, int oNewValue) {
		return Unsafe.compareAndSwapInt(pHead, oOldValue, oNewValue);
	}
	
	public void testEscape(VaporizingRow row) {
		this.slist.testEscape(row);
	}

	/**
	 * convert trx id to trx timestamp
	 * 
	 * @param force force rendering even if the tablet is not frozen
	 * @return
	 */
	synchronized void render(boolean force) {
		if (isCarbonized()) {
			return;
		}
		if (!(force || isFrozen())) {
			return;
		}
		
		// check if there is any data available for rendering
		
		long lastClosed = this.humpback.getLastClosedTransactionId();
		long startTrxId = this.startTrx.get();
		if ((startTrxId == Long.MIN_VALUE) || (startTrxId < lastClosed)) {
			return;
		}
		_log.trace("rendering {} start {}...", this.file, startTrxId);

		// reset startTrx and endTrx
		
		this.startTrx.compareAndSet(startTrxId, Long.MIN_VALUE);
		long endTrxId = this.endTrx.get();
		this.endTrx.compareAndSet(endTrxId, 0);
		
		// scan all values and realize trx id
		
		AtomicLong startSprow = new AtomicLong(Long.MAX_VALUE);
		AtomicLong endSprow = new AtomicLong(0);
		AtomicLong startTrx = new AtomicLong(Long.MIN_VALUE);
		AtomicLong endTrx = new AtomicLong(0);
		AtomicInteger count = new AtomicInteger();
		long now = getTrxMan().getCurrentVersion();
		scanAllVersion(offset -> {
			// track start sprow and end sprow
			
			ListNode node = new ListNode(this.base, offset);
			long spRow = node.getSpacePointer();
			AtomicUtil.max(endSprow, spRow);
			AtomicUtil.min(startSprow, spRow);
			
			// render trxid
			
			long version = node.getVersion();
    		if (version < -10) {
        		long trxts = getTrxMan().getTimestamp(version);
        		if (trxts < -10) {
        			if (!force) {
        				throw new HumpbackException("trxts not found for trxid: " + version);
        			}
        			AtomicUtil.max(startTrx, version);
        			AtomicUtil.min(endTrx, version);
        		}
        		else {
            		if (trxts > now) {
            			if (!force) {
            				throw new HumpbackException("incorrect trxts: " + trxts);
            			}
            		}
            		//_log.debug("render @{} trxid={} to version {}", node.getOffset(), version, trxts);
            		node.setVersion(trxts);
            		count.incrementAndGet();
        		}
    		}
			return true;
		});
		
		// update 
		
		setStartRow(startSprow.get() == Long.MAX_VALUE ? 0 : startSprow.get());
		setEndRow(endSprow.get());
		AtomicUtil.max(this.startTrx, startTrx.get());
		AtomicUtil.min(this.endTrx, endTrx.get());
		_log.trace("rendering {} ended with startTrxId={} count={}", this.file, this.startTrx.get(), count);
	}

	private void setEndRow(long value) {
		Unsafe.putLong(this.mmap.getAddress() + OFFSET_END_ROW, value);
	}

	/**
	 * 
	 * @return 0 means unknown. the tablet must be empty
	 */
	public long getStartRow() {
		long result = Unsafe.getLong(this.mmap.getAddress() + OFFSET_START_ROW);
		if (result == Long.MAX_VALUE) {
			result = 0;
		}
		return result;
	}
	
	private void setStartRow(long value) {
		Unsafe.putLong(this.mmap.getAddress() + OFFSET_START_ROW, value);
	}

	/**
	 * write the tablet to storage
	 * 
	 * @return
	 * @throws IOException
	 */
	public synchronized boolean carbonize() {
		if (isCarbonized()) {
			return true;
		}
		_log.debug("carbonizing {} ...", this.file.getName());
		
		// preparation
		
		this.heap.freeze();
		
		// if the tablet trx window is beyond the oldest active trx
		
		if (getEndTrxId() < this.humpback.getLastClosedTransactionId()) {
			return false;
		}
		
		render(false);
		setCarbonized(true);
		
		// write to disk
		
		if (this.mmap.isReadOnly()) {
			throw new CodingError();
		}
		this.mmap.force();
		_log.debug("{} is carbonized", this.file.getName());
		
		return true;
	}

	@Override
	public synchronized void close() {
		boolean isReadOnly = this.mmap.isReadOnly(); 
		super.close();
		
		// shrink the file if this is new file
		if (!isReadOnly) {
			long fileSize = this.heap.position() + HEADER_SIZE;
			try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
				raf.setLength(fileSize);
			}
			catch (IOException x) {
				_log.warn("unable to close file {}", this.file, x);
			}
		}
		this.heap = null;
	}

	private ListNode alloc(long version, long value, int next) {
		ListNode result = ListNode.alloc(heap, version, value, next);
		return result;
	}

	private void setVersion(byte v) {
		Unsafe.putByte(this.mmap.getAddress() + OFFSET_VERSION, v);
	}
	
	private void setSig(long n) {
		Unsafe.putLong(this.mmap.getAddress() + OFFSET_SIG, n);
	}
	
	void setCarbonized(boolean b) {
		Unsafe.putByteVolatile(this.mmap.getAddress() + OFFSET_CARBONIZED, (byte)(b ? 1 : 0));
	}
	
	/**
	 * 
	 * @return true if the tablet is carbonzied
	 */
	public boolean isCarbonized() {
		byte result = Unsafe.getByteVolatile(this.mmap.getAddress() + OFFSET_CARBONIZED);
		return result != 0;
	}
	
	/**
	 * 
	 * @return true if the tablet is frozen
	 */
	public boolean isFrozen() {
		return this.heap.isFull() && (this.gate.get() == 0);
	}
	
	@SuppressWarnings("unused")
	private final SpaceManager getSpaceMan() {
		return this.humpback.getSpaceManager();
	}

	private boolean lockWait(HumpbackError error, UberTimer timer, long pKey, int oHeadValue, long trxid) {
		if (error == HumpbackError.LOCK_COMPETITION) {
			this.lockwaits.incrementAndGet();
			if (!timer.isExpired()) {
				try {
					ListNode node = new ListNode(this.base, oHeadValue);
					registerLock(trxid, node.getVersion(), pKey, oHeadValue);
					Thread.sleep(0, 10000);
				}
				catch (InterruptedException e) {
				}
				return true;
			}
			else {
				_log.info(
				    "failed to acquire row lock file={} pKey={} oHeadValue={} timeout={} trxid={}",
						   this.file,
						   BytesUtil.toCompactHex(Bytes.get(heap, pKey)),
						   oHeadValue,
						   timer.getTimeOut(),
						   trxid);
				ListNode node = new ListNode(this.base, oHeadValue);
				_log.info("{}", node.toString());
			}
		}
		else if (error == HumpbackError.CONCURRENT_UPDATE) {
			this.errorConcurrencies.incrementAndGet();
		}
		return false;
	}

	private void registerLock(long requestTrxId, long lockTrxId, long pKey, long pos) {
		RowLockMonitor monitor = this.humpback.getRowLockMonitor();
		if (monitor == null) {
			return;
		}
		monitor.register(requestTrxId, lockTrxId, pKey, this.file, pos);
	}

	/**
	 * track the beginning and ending trxid
	 * 
	 * @param version
	 */
	private void trackTrxId(long trxid, int offset) {
		// _log.debug("tracking trxid {} {} @{}", getFile(), trxid, offset);
		if (trxid >= -10) {
			// if this is not trxid, just return. it is an autonomous trx
			return;
		}
		AtomicUtil.max(this.startTrx, trxid);
		AtomicUtil.min(this.endTrx, trxid);
	}

	public long getCasRetries() {
		return this.casRetries.get();
	}

	public long getLockWaits() {
		return this.lockwaits.get();
	}

	public long getCurrentUpdates() {
		return this.errorConcurrencies.get();
	}

	/**
	 * @param x
	 * @param y
	 */
	public void merge(MemTabletReadOnly x, MemTabletReadOnly y) {
		if (x.getTabletId() < y.getTabletId()) {
			// x must be newer than y in order to must sure version of the same key is following descending order
			MemTabletReadOnly t = x;
			x = y;
			y = t;
		}
		y.scan((Long pKey, ListNode node) -> {
			copy(pKey, node);
			return true;
		});
		x.scan((Long pKey, ListNode node) -> {
			copy(pKey, node);
			return true;
		});
	}

	private void copy(Long pKey, ListNode node) {
		// skip rolled back rows
		
		if (node.getVersion() == -1) {
			return;
		}
		
		// skip lock holder
		
		if (node.isLocked()) {
			return;
		}
		
		// just do it
		
		int oCopy = node.copy(this.heap);
		long pHead = this.slist.put(pKey);
		Unsafe.putInt(pHead, oCopy);
	}
}
