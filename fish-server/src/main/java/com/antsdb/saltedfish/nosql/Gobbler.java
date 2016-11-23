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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.codec.Charsets;
import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.cpp.Value;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * a new write ahead log implementation
 *  
 * @author wgu0
 */
public class Gobbler {
    static Logger _log = UberUtil.getThisLogger();
	public static short MAGIC = 0x7777;
	final static int DEFAULT_FILE_SIZE = 1024*1024*64;
	public final static int ENTRY_HEADER_SIZE = 6;

    SpaceManager spaceman;
    LogWriter writer;
    private AtomicLong spPersistence = new AtomicLong();
    
    public static enum EntryType {
        ROW,
        INDEX,
        DELETE,
        COMMIT,
        ROLLBACK,
        MESSAGE,
        TRXWINDOW,
    } 
    public static class LogEntry {
    	protected final static int HEADER_SIZE = 6;
    	protected final static int OFFSET_MAGIC = 0;
    	protected final static int OFFSET_ENTRY_TYPE = 2;
    	protected final static int OFFSET_SIZE = 3;
    	protected final static int OFFSET_TRX_ID = 6;
    	protected final static int OFFSET_TABLE_ID = 0xe;
    	
    	protected long sp;
    	protected long addr;
    	
    	public LogEntry(long sp, long addr) {
    		this.sp = sp;
    		this.addr = addr;
    	}
    	
    	protected void init(EntryType type, int size) {
    		setMagic();
    		setType(type);
    		setSize(size);
    	}
    	
    	final short getMagic() {
    		return Unsafe.getShort(addr + OFFSET_MAGIC);
    	}
    	
    	final void setMagic() {
    		Unsafe.putShort(addr + OFFSET_MAGIC, MAGIC);
    	}
    	
    	public final EntryType getType() {
    		byte bt = Unsafe.getByte(addr + OFFSET_ENTRY_TYPE);
    		switch (bt) {
    		case 0:
    			return EntryType.ROW;
    		case 1:
    			return EntryType.INDEX;
    		case 2:
    			return EntryType.DELETE;
    		case 3:
    			return EntryType.COMMIT;
    		case 4:
    			return EntryType.ROLLBACK;
    		case 5:
    			return EntryType.MESSAGE;
    		case 6:
    			return EntryType.TRXWINDOW;
    		default:
    			throw new IllegalArgumentException();
    		}
    	}
    	
    	final void setType(EntryType type) {
    		byte bt = (byte)type.ordinal();
    		Unsafe.putByte(addr + OFFSET_ENTRY_TYPE, bt);
    	}
    	
    	public final int getSize() {
    		return Unsafe.getInt3(addr + OFFSET_SIZE);
    	}
    	
    	final void setSize(int size) {
    		Unsafe.putInt3(addr + OFFSET_SIZE, size);
    	}

    	final long getAddress() {
    		return this.addr;
    	}
    	
    	public final long getSpacePointer() {
			return this.sp;
		}
    }
    
    public final static class PutEntry extends LogEntry {
    	static PutEntry alloc(SpaceManager spaceman, int size) {
    		long sp = spaceman.alloc(HEADER_SIZE + size);
    		long p = spaceman.toMemory(sp);
    		PutEntry entry = new PutEntry(sp, p);
    		entry.init(EntryType.ROW, size);
    		return entry;
    	}

    	PutEntry(long sp, long addr) {
    		super(sp, addr);
    	}
    	
    	public long getRowPointer() {
    		return this.addr + ENTRY_HEADER_SIZE;    		
    	}

		public long getRowSpacePointer() {
			return this.sp + ENTRY_HEADER_SIZE;
		}
		
		public long getTrxId() {
			long pRow = getRowPointer();
			long trxid = Row.getVersion(pRow);
			return trxid;
		}
    }
    
    public final static class DeleteEntry extends LogEntry {
    	protected final static int OFFSET_KEY = 0x12;

    	static DeleteEntry alloc(SpaceManager spaceman, int tableId, long trxid, long pKey, int length) {
    		long sp = spaceman.alloc(HEADER_SIZE + 4 + 8 + length);
    		long p = spaceman.toMemory(sp);
    		DeleteEntry entry = new DeleteEntry(sp, p);
    		entry.init(EntryType.DELETE, 4 + 8 + length);
    		entry.setTrxId(trxid);
    		entry.setTableId(tableId);
			Unsafe.copyMemory(pKey, entry.getKeyAddress(), length);
    		return entry;
    	}

    	DeleteEntry(long sp, long addr) {
    		super(sp, addr);
    	}

    	public long getTrxid() {
    		return Unsafe.getLong(this.addr + OFFSET_TRX_ID);
    	}
    	
    	void setTrxId(long trxid) {
    		Unsafe.putLong(this.addr + OFFSET_TRX_ID, trxid);
    	}

    	public int getTableId() {
    		return Unsafe.getInt(this.addr + OFFSET_TABLE_ID);
    	}
    	
    	void setTableId(int tableId) {
    		Unsafe.putInt(this.addr + OFFSET_TABLE_ID, tableId);
    	}

    	public long getKeyAddress() {
    		return this.addr + OFFSET_KEY;
    	}
    	
    }
    
    public final static class IndexEntry extends LogEntry {
    	protected final static int OFFSET_MISC = 0x12;
    	protected final static int OFFSET_INDEX_KEY = 0x13;
    	
    	static IndexEntry alloc(SpaceManager spaceman, int size) {
    		long sp = spaceman.alloc(HEADER_SIZE + size);
    		long p = spaceman.toMemory(sp);
    		IndexEntry entry = new IndexEntry(sp, p);
    		entry.init(size);
    		return entry;
    	}
    	
		static IndexEntry alloc(
				SpaceManager spaceman, 
				int tableId, 
				long trxid, 
				long pIndexKey, 
				long pRowKey, 
				byte misc) {
			int indexKeySize = KeyBytes.getRawSize(pIndexKey);
			int rowKeySize = (pRowKey != 0) ? KeyBytes.getRawSize(pRowKey) : 1;
			IndexEntry entry = alloc(spaceman, 8 + 4 + 1 + indexKeySize + rowKeySize);
			entry.setTableId(tableId);
			entry.setTrxId(trxid);
			entry.setMisc(misc);
			Unsafe.copyMemory(pIndexKey, entry.getIndexKeyAddress(), indexKeySize);
			if (pRowKey != 0) {
				Unsafe.copyMemory(pRowKey, entry.getIndexKeyAddress() + indexKeySize, rowKeySize);
			}
			else {
				Unsafe.putByte(entry.getIndexKeyAddress() + indexKeySize, Value.FORMAT_NULL);
			}
			return entry;
		}

    	IndexEntry(long sp, long addr) {
    		super(sp, addr);
    	}
    	
    	protected void init(int size) {
    		super.init(EntryType.INDEX, size);
    	}

    	public long getTrxid() {
    		return Unsafe.getLong(this.addr + OFFSET_TRX_ID);
    	}
    	
    	void setTrxId(long trxid) {
    		Unsafe.putLong(this.addr + OFFSET_TRX_ID, trxid);
    	}

    	public int getTableId() {
    		return Unsafe.getInt(this.addr + OFFSET_TABLE_ID);
    	}
    	
    	void setTableId(int tableid) {
    		Unsafe.putInt(this.addr + OFFSET_TABLE_ID, tableid);
    	}

    	public long getIndexKeyAddress() {
    		return this.addr + OFFSET_INDEX_KEY;
    	}
    	
    	public long getRowKeyAddress() {
    		long p = getIndexKeyAddress();
    		int indexKeySize = KeyBytes.getRawSize(p);
    		long pRowKey = p + indexKeySize;
    		if (Value.getFormat(null, pRowKey) == Value.FORMAT_NULL) {
    			return 0;
    		}
    		else {
    			return pRowKey;
    		}
    	}
    	
    	public byte getMisc() {
    		byte value =  Unsafe.getByte(this.addr + OFFSET_MISC);
    		return value;
    	}
    	
    	public void setMisc(byte value) {
    		Unsafe.putByte(this.addr + OFFSET_MISC, value);
    	}
    }
    
    public final static class CommitEntry extends LogEntry {
    	protected final static int OFFSET_VERSION = HEADER_SIZE + 8;
    	
		static CommitEntry alloc(SpaceManager spaceman, long trxid, long trxts) {
    		long sp = spaceman.alloc(HEADER_SIZE + 8 + 8);
    		long p = spaceman.toMemory(sp);
    		CommitEntry entry = new CommitEntry(sp, p);
    		entry.init(EntryType.COMMIT, 8 + 8);
    		entry.setTrxId(trxid);
    		entry.setVersion(trxts);
    		return entry;
		}
		
    	CommitEntry(long sp, long addr) {
    		super(sp, addr);
    	}

    	public long getTrxid() {
    		return Unsafe.getLong(this.addr + OFFSET_TRX_ID);
    	}
    	
    	void setTrxId(long trxid) {
    		Unsafe.putLong(this.addr + OFFSET_TRX_ID, trxid);
    	}

    	public long getVersion() {
    		return Unsafe.getLong(this.addr + OFFSET_VERSION);
    	}
    	
    	void setVersion(long version) {
    		Unsafe.putLong(this.addr + OFFSET_VERSION, version);
    	}

    }
    
    public final static class RollbackEntry extends LogEntry {
		static RollbackEntry alloc(SpaceManager spaceman, long trxid) {
    		long sp = spaceman.alloc(HEADER_SIZE + 8);
    		long p = spaceman.toMemory(sp);
    		RollbackEntry entry = new RollbackEntry(sp, p);
    		entry.init(EntryType.ROLLBACK, 8);
    		entry.setTrxId(trxid);
    		return entry;
		}
		
    	RollbackEntry(long sp, long addr) {
    		super(sp, addr);
    	}

    	public long getTrxid() {
    		return Unsafe.getLong(this.addr + OFFSET_TRX_ID);
    	}
    	
    	void setTrxId(long trxid) {
    		Unsafe.putLong(this.addr + OFFSET_TRX_ID, trxid);
    	}

    }
    
    public final static class TransactionWindowEntry extends LogEntry {
		static TransactionWindowEntry alloc(SpaceManager spaceman, long trxid) {
    		long sp = spaceman.alloc(HEADER_SIZE + 8);
    		long p = spaceman.toMemory(sp);
    		TransactionWindowEntry entry = new TransactionWindowEntry(sp, p);
    		entry.init(EntryType.TRXWINDOW, 8);
    		entry.setTrxId(trxid);
    		return entry;
		}
		
		TransactionWindowEntry(long sp, long addr) {
    		super(sp, addr);
    	}

    	public long getTrxid() {
    		return Unsafe.getLong(this.addr + OFFSET_TRX_ID);
    	}
    	
    	void setTrxId(long trxid) {
    		Unsafe.putLong(this.addr + OFFSET_TRX_ID, trxid);
    	}

    }
    
    public final static class MessageEntry extends LogEntry {
    	protected final static int OFFSET_MESSAGE = 6;
    	
		static MessageEntry alloc(SpaceManager spaceman, String message) {
			byte[] bytes = message.getBytes(Charsets.UTF_8);
    		long sp = spaceman.alloc(HEADER_SIZE + bytes.length);
    		long p = spaceman.toMemory(sp);
    		MessageEntry entry = new MessageEntry(sp, p);
    		entry.init(EntryType.MESSAGE, bytes.length);
    		for (int i=0; i<bytes.length; i++) {
    			Unsafe.putByte(p + OFFSET_MESSAGE + i, bytes[i]);
    		}
    		return entry;
		}
		
		MessageEntry(long sp, long addr) {
    		super(sp, addr);
    	}

    	public String getMessage() {
    		int size = getSize();
    		byte[] bytes = new byte[size];
    		for (int i=0; i<size; i++) {
    			bytes[i] = Unsafe.getByte(this.addr + OFFSET_MESSAGE + i);
    		}
    		return new String(bytes, Charsets.UTF_8);
    	}
    }
    
    public Gobbler(SpaceManager spaceman, boolean enableWriter) throws IOException {
    	this.spaceman = spaceman;
    	if (!this.spaceman.isReadOnly() && enableWriter) {
        	this.writer = new LogWriter(spaceman, this);
        	this.writer.start();
    	}
	}

    private void updatePersistencePointer(long spStart) {
    	for (;;) {
    		long sp = this.spPersistence.get();
    		if (spStart >= sp) {
    			break;
    		}
			if (this.spPersistence.compareAndSet(sp, spStart)) {
				break;
			}
    	}
	}

    public void logMessage(String message) {
    	MessageEntry entry = MessageEntry.alloc(spaceman, message);
    	long sp = entry.getSpacePointer();
    	updatePersistencePointer(sp);
    }
    
	public void logTransactionWindow(long trxid) {
		TransactionWindowEntry entry = TransactionWindowEntry.alloc(spaceman, trxid);
    	long sp = entry.getSpacePointer();
    	updatePersistencePointer(sp);
	}

	public void logCommit(long trxid, long trxts) {
		CommitEntry entry = CommitEntry.alloc(spaceman, trxid, trxts);
    	long sp = entry.getSpacePointer();
    	updatePersistencePointer(sp);
    }

    void logRollback(long trxid) {
		RollbackEntry entry = RollbackEntry.alloc(spaceman, trxid);
    	long sp = entry.getSpacePointer();
    	updatePersistencePointer(sp);
    }

	long logIndex(int tableId, long trxid, long pIndexKey, long pRowKey, byte misc) {
		IndexEntry entry = IndexEntry.alloc(spaceman, tableId, trxid, pIndexKey, pRowKey, misc);
		long sp = entry.getSpacePointer();
    	updatePersistencePointer(sp);
    	return sp + ENTRY_HEADER_SIZE;
	}
	
	long logRow(VaporizingRow row, int tableId) {
		PutEntry entry = PutEntry.alloc(spaceman, row.getSize());
		long sp = entry.getSpacePointer();
		long p = entry.getRowPointer();
    	Row.from(p, row, tableId);
    	updatePersistencePointer(sp);
    	return sp + ENTRY_HEADER_SIZE;
    }
    
	long logRow(long pRow, int length) {
		PutEntry entry = PutEntry.alloc(spaceman, length);
		long sp = entry.getSpacePointer();
		long p = entry.getRowPointer();
    	Unsafe.copyMemory(pRow, p, length);
    	updatePersistencePointer(sp);
    	return sp + ENTRY_HEADER_SIZE;
    }
    
	public long logDelete(long trxid, int tableId, long pKey, int length) {
		DeleteEntry entry = DeleteEntry.alloc(spaceman, tableId, trxid, pKey, length);
		long sp = entry.getSpacePointer();
    	updatePersistencePointer(sp);
    	return sp + ENTRY_HEADER_SIZE;
	}

	public long replayFromRowPointer(long spStartRow, ReplayHandler handler, boolean inclusive) throws Exception {
		long spStart = spStartRow - ENTRY_HEADER_SIZE;
		return replay(spStart, inclusive, handler);
	}
	
	public long replay(long spStart, boolean inclusive, ReplayHandler handler) throws Exception {
		return replay(spStart, Long.MAX_VALUE, inclusive, handler); 
	}

	/**
	 * 
	 * @param spStart
	 * @param spEnd
	 * @param inclusive
	 * @param handler
	 * @return end sp
	 * @throws Exception
	 */
	public long replay(long spStart, long spEnd, boolean inclusive, ReplayHandler handler) throws Exception {
		long end = -1;
		
		if (this.spaceman.getAllocationPointer() <= spStart) {
			// log is empty
			return spStart;
		}
		
		// start point must be valid
		
		long p = this.spaceman.toMemory(spStart);
		if (p == 0) {
			return end;
		}
		if (Unsafe.getShort(p) != MAGIC) {
			throw new IllegalArgumentException();
		}
		
		// loop
		
		for (long sp=spStart; sp!=-1 & sp<spEnd;) {
			
			// verify signature and move to next segment if necessary
			
			p = this.spaceman.toMemory(sp);
			if (Unsafe.getShort(p) != MAGIC) {
				sp = this.spaceman.nextSegment(sp);
				if (sp == -1) {
					// end of space
					return end;
				}
				p = this.spaceman.toMemory(sp);
				if (Unsafe.getShort(p) != MAGIC) {
					// no data found
					return end;
				}
			}
			
			// callback
			
			LogEntry e = new LogEntry(sp, p);
			EntryType type = e.getType();
			int length = e.getSize();
			if (_log.isTraceEnabled()) {
				_log.trace("recover type {} length {} @ {}", type, length, sp);
			}
			if ((sp != spStart) || inclusive) {
				end = sp;
				switch (type) {
					case ROW: {
						PutEntry entry = new PutEntry(sp, p);
						handler.all(entry);
						handler.put(entry);
						break;
					}
					case DELETE: {
						DeleteEntry entry = new DeleteEntry(sp, p);
						handler.all(entry);
						handler.delete(entry);
						break;
					}
					case COMMIT: {
						CommitEntry entry = new CommitEntry(sp, p);
						handler.all(entry);
						handler.commit(entry);
						break;
					}
					case ROLLBACK: {
						RollbackEntry entry = new RollbackEntry(sp, p);
						handler.all(entry);
						handler.rollback(entry);
						break;
					}
					case INDEX: {
						IndexEntry entry = new IndexEntry(sp, p);
						handler.all(entry);
						handler.index(entry);
						break;
					}
					case MESSAGE: {
						MessageEntry entry = new MessageEntry(sp, p);
						handler.all(entry);
						handler.message(entry);
						break;
					}
					case TRXWINDOW: {
						TransactionWindowEntry entry = new TransactionWindowEntry(sp, p);
						handler.all(entry);
						handler.transactionWindow(entry);
						break;
					}
				}
			}
			sp = this.spaceman.plus(sp, length + ENTRY_HEADER_SIZE, 2);
			if (sp == Long.MAX_VALUE) {
				// end of space
				break;
			}
		}
		return end;
	}
	
	public void close() {
		_log.info("closing gobbler ...");
		if (this.writer != null) {
			this.writer.close();
			try {
				this.writer.join();
			}
			catch (InterruptedException ignored) {
			}
			this.writer = null;
		}
	}

	public AtomicLong getPersistencePointer() {
		return this.spPersistence;
	}

	/**
	 * get earliest possible space pointer
	 * 
	 * @return
	 */
	public long getStartSp() {
		return this.spaceman.getStartSp();
	}

	/**
	 * return -1 if there is no valid sp found meaning this is an empty database
	 * @throws Exception 
	 */
	public long getLatestSp() {
		long sp = this.spaceman.getAllocationPointer();
		int spaceId = SpaceManager.getSpaceId(sp);
		long spaceStartSp = this.spaceman.getSpaceStartSp(spaceId);
		if (spaceStartSp == sp) {
			// if current space is empty, wait a little
			try {
				Thread.sleep(1000);
			}
			catch (InterruptedException e) {
			}
		}
		AtomicLong result = new AtomicLong(-1);
		try {
			this.replay(spaceStartSp, true, new ReplayHandler(){
				@Override
				public void all(LogEntry entry) {
					result.set(entry.getSpacePointer());
				}
			});
		}
		catch (Exception ignored) {
		}
		return result.get();
	}

}
