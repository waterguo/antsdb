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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.cpp.OutOfHeapMemory;
import com.antsdb.saltedfish.nosql.ConcurrentLinkedList.Node;
import com.antsdb.saltedfish.util.UberUtil;

public final class MemTable extends MemTableReadOnly {
    final static int MAGIC = 0x0211;
    final static int VERSION = 1;
    final static int INITIAL_FILE_SIZE = 16 * 1024;

	static final Logger _log = UberUtil.getThisLogger();

    Humpback owner;
    MemTablet tablet;
	private int tabletSize;
	private boolean isClosed;
	private Map<Integer, MemTabletReadOnly> retired = Collections.synchronizedMap(new HashMap<>());

    public MemTable(Humpback owner, File ns, int tableId, int tabletSize) {
    	super(owner.getSpaceManager(), tableId);
    	this.ns = ns;
    	this.tabletSize = tabletSize;
        this.owner = owner;
        this.tableId = tableId;
        this.ns = ns;
    }
    
	public void open() throws IOException {
		open(false);
	}
	
	/**
	 * open the mem table
	 * 
	 * @param deleteCorruptedFile delete tablets not closed properly in last shutdown if true
	 * @throws IOException 
	 */
	public void open(boolean deleteCorruptedFile) throws IOException {
    	// find the matching files
    	
    	List<File> files = new ArrayList<>();
    	for (File i:ns.listFiles()) {
            int fileTableId = MemTablet.getTableId(i);
            if (fileTableId != this.tableId) {
            	continue;
            }
            files.add(i);
    	}
    	
    	// must ordered by tablet id
    	
    	Collections.sort(files);
    	Collections.reverse(files);
    	
    	// load tablets

    	for (File i:files) {
            MemTablet tablet = new MemTablet(owner, i);
            if (deleteCorruptedFile && !tablet.isCarbonized()) {
            	_log.warn("{} is not properly closed. deleting ... {}", i, i.exists());
            	tablet.close();
            	if (!i.delete()) {
            		throw new HumpbackException("unable to delete file " + i.toString());
            	}
            	continue;
            }
            this.tablets.addLast(tablet);
    	}
	}

	public HumpbackError insertIndex(long trxid, long pIndexKey, long pRowKey, byte misc, int timeout) {
		if (this.tablet == null) {
			grow(null);
		}
		for (;;) {
			MemTablet current = this.tablet;
			try {
				long sp = this.owner.getGobbler().logIndex(tableId, trxid, pIndexKey, pRowKey, misc);
		    	return current.insertIndex(pIndexKey, trxid, pRowKey, this.tablets, sp, misc, timeout);
			}
			catch (OutOfHeapMemory x) {
				grow(current);
			}
		}
	}
	
	public HumpbackError insertIndex_nologging(
			long trxid, 
			long pIndexKey, 
			long pRowKey, 
			long sp, 
			byte misc, 
			int timeout) {
		if (this.tablet == null) {
			grow(null);
		}
		for (;;) {
			MemTablet current = this.tablet;
			try {
		    	return current.insertIndex(pIndexKey, trxid, pRowKey, this.tablets, sp, misc, timeout);
			}
			catch (OutOfHeapMemory x) {
				grow(current);
			}
		}
	}

	public HumpbackError insert(VaporizingRow row, int timeout) {
		if (this.tablet == null) {
			grow(null);
		}
		for (;;) {
			MemTablet current = this.tablet;
			try {
		    	return current.insert(row, this.tablets, timeout);
			}
			catch (OutOfHeapMemory x) {
				grow(current);
			}
		}
    }
    
	public HumpbackError update(VaporizingRow row, long oldVersion, int timeout) {
		if (this.tablet == null) {
			grow(null);
		}
		for (;;) {
			MemTablet current = this.tablet;
			try {
		    	return current.update(row, oldVersion, this.tablets, timeout);
			}
			catch (OutOfHeapMemory x) {
				grow(current);
			}
		}
    }
    
    public HumpbackError delete(long trxid, long pKey, int timeout) {
		if (this.tablet == null) {
			grow(null);
		}
		for (;;) {
			MemTablet current = this.tablet;
			try {
		    	long sprow = logDelete(trxid, pKey);
		    	return current.delete(pKey, trxid, this.tablets, sprow, timeout);
			}
			catch (OutOfHeapMemory x) {
				grow(current);
			}
		}
    }
    
    public HumpbackError deleteNoLogging(long trxid, long pKey, long sprow, int timeout) {
		if (this.tablet == null) {
			grow(null);
		}
		for (;;) {
			MemTablet current = this.tablet;
			try {
		    	return current.delete(pKey, trxid, this.tablets, sprow, timeout);
			}
			catch (OutOfHeapMemory x) {
				grow(current);
			}
		}
    }
    
    public HumpbackError put(VaporizingRow row) {
		if (this.tablet == null) {
			grow(null);
		}
		for (;;) {
			MemTablet current = this.tablet;
			try {
				return current.put(row, this.tablets);
			}
			catch (OutOfHeapMemory x) {
				grow(current);
			}
		}
    }
    
	public HumpbackError putNoLogging(long trxid, long pKey, long spRow) {
		if (this.tablet == null) {
			grow(null);
		}
		for (;;) {
			MemTablet current = this.tablet;
			try {
				return current.putNoLogging(pKey, trxid, spRow, this.tablets);
			}
			catch (OutOfHeapMemory x) {
				grow(current);
			}
		}
	}
	
    private long logDelete(long trxid, long pKey) {
    	int length = KeyBytes.getRawSize(pKey);
    	return this.owner.getGobbler().logDelete(trxid, this.tableId, pKey, length);
    }
    
	public HumpbackError lock(long trxid, long pKey, int timeout) {
		if (this.tablet == null) {
			grow(null);
		}
		for (;;) {
			MemTablet current = this.tablet;
			try {
				return current.lock(trxid, pKey, this.tablets, timeout);
			}
			catch (OutOfHeapMemory x) {
				grow(current);
			}
		}
	}

    boolean carbonize() throws Exception {
    	for (MemTabletReadOnly i:getTabletsReadOnly()) {
    		if (!(i instanceof MemTablet)) {
    			continue;
    		}
    		MemTablet tablet = (MemTablet) i;
    		if (!tablet.carbonize()) {
    			return false;
    		}
    	}
    	return true;
    }
    
    public void truncate() {
        drop();
        resetGrowth();
		grow(null);
    }

	public void testEscape(VaporizingRow row) {
		if (tablet != null) {
			this.tablet.testEscape(row);
		}
	}

	public synchronized void close() {
		this.isClosed = true;
		for (MemTabletReadOnly i:this.tablets) {
			i.close();
		}
		this.tablets.clear();
		this.tablet = null;
	}
	
	public synchronized void drop() {
		close();
		for (MemTablet i:getTablets()) {
			i.drop();
		}
		this.tablets.clear();
		this.tablet = null;
	}

	@Override
	protected MemTabletReadOnly extend(MemTabletReadOnly filled) {
		try {
			int tabletId = 0;
			if (getTablets().size > 0) {
				tabletId = getTablets().getFirst().getTabletId() + 2;
			}
	    	MemTablet newone;
	    	int filesize = (tabletId == 0) ? INITIAL_FILE_SIZE : this.tabletSize;
			newone = new MemTablet(owner, this.ns, this.tableId, tabletId, filesize);
	    	this.tablet = newone;
	    	this.tablets.addFirst(newone);
	    	return newone;
		}
		catch (IOException x) {
			throw new HumpbackException(x);
		}
	}
    
    boolean validate() {
    	boolean result = true;
    	for (MemTablet i:getTablets()) {
    		result = result && i.validate();
    	}
    	return result;
    }

	public long getEndRowSpacePointer() {
		for (MemTablet i:getTablets()) {
			long end = i.getEndRow();
			if (end != 0) {
				return end;
			}
		}
		return 0;
	}

	@Override
	public String toString() {
		String text = String.format("%s/%08x", this.ns.toString(), this.tableId);
		return text;
	}

	public void render(long endTrxId) {
		for (MemTabletReadOnly tablet:getTabletsReadOnly()) {
			if (!(tablet instanceof MemTablet)) {
				continue;
			}
			((MemTablet)tablet).render(endTrxId);
		}
	}

	public synchronized void carbonizeIfPossible() throws IOException {
		for (MemTabletReadOnly i:this.tablets) {
			if (i instanceof MemTablet) {
				MemTablet tablet = (MemTablet)i;
				if (tablet.isCarbonized()) {
					continue;
				}
				if (!tablet.isFrozen()) {
					continue;
				}
				tablet.carbonize();
			}
		}
	}

	public boolean isPureEmpty() {
		if (this.tablets.size() > 1) {
			return false;
		}
		return (this.tablet != null) ? this.tablet.isPureEmpty() : true;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public ConcurrentLinkedList<MemTablet> getTablets() {
		return (ConcurrentLinkedList)this.tablets;
	}

	public MemTablet getCurrentTablet() {
		return this.tablet;
	}

	public synchronized void compact() throws IOException {
		if (isClosed) {
			return;
		}
		
		// find candidate
		
		ConcurrentLinkedList.Node<MemTabletReadOnly> node = findCompactCandidate();
		if (node == null) {
			return;
		}
		
		// double check existence of new file
		
		MemTabletReadOnly lead = node.data;
		int babyTabletId = lead.getTabletId()-1;
		File babyFile = MemTablet.getFile(ns, this.tableId, babyTabletId);
		if (babyFile.exists()) {
			_log.error("compact failed due to existence of file {}", babyFile);
			return;
		}
		
		// start compacting
		
		MemTabletReadOnly x = node.next.data;
		MemTabletReadOnly y = node.next.next.data;
		long size = getSizeOfNextTwoTablets(node) * 3 / 2;
		if (size >= Integer.MAX_VALUE) {
			// file is too big, give up
			return;
		}
		_log.debug("start merging {} and {} to {} ...", x.getTabletId(), y.getTabletId(),babyFile);
		MemTabletReadOnly baby = new MemTablet(this.owner, this.ns, this.tableId, babyTabletId, (int)size);
		boolean success = false;
		try {
			((MemTablet)baby).merge(node.next.data, node.next.next.data);
			((MemTablet)baby).carbonize();
			baby.close();
			baby = new MemTabletReadOnly(babyFile); 
			_log.debug(
					"merging {} with size {} and {} with size {} to {} with size {} is finished", 
					x.getTabletId(), 
					x.getFile().length(),
					y.getTabletId(),
					y.getFile().length(),
					babyFile,
					babyFile.length());
			success = true;
		}
		finally {
			if (!success) {
				baby.close();
				babyFile.delete();
			}
		}
		
		// update linked list
		
		ConcurrentLinkedList.Node<MemTabletReadOnly> babyNode = super.tablets.insert(node, baby);
		this.retired.put(x.getTabletId(), x);
		this.retired.put(y.getTabletId(), y);
		super.tablets.deleteNext(babyNode);
		super.tablets.deleteNext(babyNode);
		this.owner.getJobManager().schedule(5, TimeUnit.MINUTES, () -> {
			_log.debug("deleting {} due to compacting", x.getFile());
			x.drop();
			y.drop();
			this.retired.remove(x.getTabletId());
			this.retired.remove(y.getTabletId());
		});
	}
		
	private ConcurrentLinkedList.Node<MemTabletReadOnly> findCompactCandidate() {
		// find the two consecutive tablets with smallest size
		
		long minSize = Integer.MAX_VALUE;
		ConcurrentLinkedList.Node<MemTabletReadOnly> tabletWithMinSize = null;
		for (ConcurrentLinkedList.Node<MemTabletReadOnly> i=super.tablets.getFirstNode(); i!=null; i=i.next) {
			if (!isNextTabletIdAvailable(i)) {
				continue;
			}
			long size = getSizeOfNextTwoTablets(i);
			if (size < minSize) {
				minSize = size;
				tabletWithMinSize = i;
			}
		}
		return tabletWithMinSize;
	}

	private long getSizeOfNextTwoTablets(Node<MemTabletReadOnly> node) {
		if (node.next == null) {
			return Long.MAX_VALUE; 
		}
		if (node.next.next == null) {
			return Long.MAX_VALUE;
		}
		return node.next.data.getFile().length() + node.next.next.data.getFile().length();
	}

	private boolean isNextTabletIdAvailable(Node<MemTabletReadOnly> node) {
		if (node.next == null) {
			return false;
		}
		int mergedTabletId = node.data.getTabletId() - 1;
		MemTabletReadOnly nextTablet = node.next.data;
		if (nextTablet.getTabletId() >= mergedTabletId) {
			// there is no space between current node and next node
			return false;
		}
		if (this.retired.containsKey(mergedTabletId)) {
			// candidate id is taken by a retired tablet
			return false;
		}
		return true;
	}

}
