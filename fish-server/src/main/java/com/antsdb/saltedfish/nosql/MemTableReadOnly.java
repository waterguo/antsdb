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
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.VariableLengthLongComparator;
import com.antsdb.saltedfish.util.ScalableData;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * 
 * @author wgu0
 */
public class MemTableReadOnly extends ScalableData<MemTabletReadOnly> {
    final static int MAGIC = 0x0211;
    final static int VERSION = 1;

	static final Logger _log = UberUtil.getThisLogger();
    static final VariableLengthLongComparator _comp = new VariableLengthLongComparator();
    
    int tableId;
	ConcurrentLinkedList<MemTabletReadOnly> tablets = new ConcurrentLinkedList<>();
	File ns;
	private SpaceManager spaceman;
    
    static class Scanner implements RowIterator {
    	List<MemTablet.Scanner> upstreams = new ArrayList<>();
    	long counter = 0;
    	MemTablet.Scanner lastFetched = null;
    	long pRow = 0;
    	long version = 0;
    	boolean eof = false;
		SpaceManager spaceman;
		boolean isAscending = true;
    	
		@Override
		public boolean next() {
			if (this.eof) {
				return false;
			}
			
			// 1 step forward
			
			if (lastFetched != null) {
				lastFetched.next();
			}
			else {
				for (MemTablet.Scanner i:upstreams) {
					i.next();
				}
			}
			
			// find the smallest key in ascending order. or the greatest in descending order 
			
			long pKey = 0;
			for (MemTablet.Scanner i:upstreams) {
				if (i.eof()) {
					continue;
				}
				long pKeyI = i.getKeyPointer();
				if (pKey == 0) {
					this.pRow = i.getRowPointer();
					this.lastFetched = i;
					this.version = i.getVersion();
					pKey = pKeyI;
					continue;
				}
				int cmp = compare(pKeyI, pKey);
				if (cmp < 0) {
					this.pRow = i.getRowPointer();
					this.lastFetched = i;
					this.version = i.getVersion();
					pKey = pKeyI;
				}
				else if (cmp == 0) {
					// same key. one step forward
					i.next();
				}
			}
			
			// eof detection
			
			if (pKey == 0) {
				this.eof = true;
				this.pRow = 0;
				this.lastFetched = null;
				return false;
			}
			
			// valid value found
			
			return true;
		}

		private int compare(long pKeyX, long pKeyY) {
			int result = MemTableReadOnly._comp.compare(pKeyX, pKeyY);
			if (!this.isAscending) {
				result = -result;
			}
			return result;
		}

		@Override
		public long getRowScanned() {
			return this.counter;
		}

		@Override
		public void rewind() {
			for (MemTablet.Scanner i:upstreams) {
				i.rewind();
			}
			this.eof = false;
			this.pRow = 0;
			this.lastFetched = null;
		}

		@Override
		public long getRowPointer() {
			return this.pRow;
		}

		@Override
		public long getVersion() {
			return this.version;
		}

		@Override
		public Row getRow() {
			long spRow = getRowPointer();
			Row row = Row.fromSpacePointer(this.spaceman, spRow, this.version);
			return row;
		}

		@Override
		public long getRowKeyPointer() {
			return this.lastFetched.getRowKeyPointer();
		}

		@Override
		public void close() {
		}

		@Override
		public long getKeyPointer() {
			return this.lastFetched.getKeyPointer();
		}

		@Override
		public boolean isRow() {
			return this.lastFetched.isRow();
		}

		@Override
		public long getIndexSuffix() {
			return this.lastFetched.getIndexSuffix();
		}

		@Override
		public boolean eof() {
			return this.eof;
		}

		@Override
		public byte getMisc() {
			return this.lastFetched.getMisc();
		}
    }
    
    public MemTableReadOnly(SpaceManager spaceman, File ns, int tableId) {
    	this(spaceman, tableId);
        this.ns = ns;
    }
    
    protected MemTableReadOnly(SpaceManager spaceman, int tableId) {
    	this.spaceman = spaceman;
        this.tableId = tableId;
    }
    
	@Override
	public String toString() {
		String text = String.format("%s/%08x", this.ns.toString(), this.tableId);
		return text;
	}

	public int getId() {
		return this.tableId;
	}

    /**
     * 
     * @param trx update transaction id, can be 0
     * @param trxts read transaction timestamp
     * @param key
     * @return space pointer to the row. 0 means not found
     */
    public long get(long trxid, long version, long pKey) {
    	for (MemTabletReadOnly ii:this.tablets) {
    		long pRow = ii.get(trxid, version, pKey, 0);
    		if (pRow != 0) {
    	        return Row.isTombStone(pRow) ? 0 : pRow;
    		}
    	}
    	return 0;
    }
    
    public Row getRow(long trxid, long version, long pKey) {
    	RowKeeper keeper = new RowKeeper();
    	for (MemTabletReadOnly ii:this.tablets) {
    		int result = ii.getRow(keeper, trxid, version, pKey);
    		if (result == 1) {
        		return Row.fromSpacePointer(this.spaceman, keeper.sprow, keeper.version);
    		}
    		else if (result == -1) {
    			// tomb stone
    			break;
    		}
    	}
    	return null;
    }
    
	public long getIndex(long trxid, long version, long pKey) {
    	for (MemTabletReadOnly ii:this.tablets) {
    		long pRowKey = ii.getIndex(trxid, version, pKey);
    		if (pRowKey != 0) {
    	        return Row.isTombStone(pRowKey) ? 0 : pRowKey;
    		}
    	}
    	return 0;
	}
	
    public Scanner scan(
            long trxid, 
            long version,
            long pKeyStart, 
            boolean fromInclusive, 
            long pKeyEnd, 
            boolean toInclusive, 
            boolean isAscending) {
		Scanner scanner = new Scanner();
		scanner.isAscending = isAscending;
		scanner.spaceman = this.spaceman;
		for (MemTabletReadOnly i:this.tablets) {
			MemTablet.Scanner upstream;
			upstream = i.scan(trxid, version, pKeyStart, fromInclusive, pKeyEnd, toInclusive, isAscending);
			if (upstream != null) {
				scanner.upstreams.add(upstream);
			}
		}
        return scanner;
    }
    
	long size() {
		long size = 0;
		for (MemTabletReadOnly tablet:this.tablets) {
			size += tablet.size();
		}
		return size;
	}

	public ConcurrentLinkedList<MemTabletReadOnly> getTabletsReadOnly() {
		return this.tablets;
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
    	
    	// load tablets

    	for (File i:files) {
    		MemTabletReadOnly tablet = new MemTabletReadOnly(i);
            this.tablets.addFirst(tablet);
    	}
	}

	public synchronized void close() {
		ConcurrentLinkedList<MemTabletReadOnly> list = new ConcurrentLinkedList<MemTabletReadOnly>(this.tablets);
		this.tablets.clear();
		for (MemTabletReadOnly i:list) {
			i.close();
		}
	}
	
    boolean validate() {
    	boolean result = true;
    	for (MemTabletReadOnly i:this.tablets) {
    		result = result && i.validate();
    	}
    	return result;
    }

	public long getEndRowSpacePointer() {
		for (MemTabletReadOnly i:this.tablets) {
			long end = i.getEndRow();
			if (end != 0) {
				return end;
			}
		}
		return 0;
	}

	/**
	 * 
	 * @return Long.MIN_VALUE when there is pending data
	 */
	public long getStartTrxId() {
		long startTrxId = Long.MIN_VALUE;
		for (MemTabletReadOnly tablet:this.tablets) {
			long tabletStartTrxId = tablet.getStartTrxId();
			if (tabletStartTrxId == 0) {
				continue;
			}
			startTrxId = Math.max(startTrxId, tabletStartTrxId);
		}
		return startTrxId;
	}

	@Override
	protected MemTabletReadOnly extend(MemTabletReadOnly filled) {
		throw new NotImplementedException();
	}

}
