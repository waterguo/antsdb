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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.function.IntPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.BluntHeap;
import com.antsdb.saltedfish.cpp.FileOffset;
import com.antsdb.saltedfish.cpp.FishSkipList;
import com.antsdb.saltedfish.cpp.KeyBytes;
import com.antsdb.saltedfish.cpp.OutOfHeapMemory;
import com.antsdb.saltedfish.cpp.SkipListScanner;
import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.cpp.VariableLengthLongComparator;
import com.antsdb.saltedfish.util.AtomicUtil;
import com.antsdb.saltedfish.util.BytesUtil;
import com.antsdb.saltedfish.util.CodingError;
import com.antsdb.saltedfish.util.ConsoleHelper;
import com.antsdb.saltedfish.util.LatencyDetector;
import com.antsdb.saltedfish.util.LongLong;

import static com.antsdb.saltedfish.util.UberFormatter.*;
import com.antsdb.saltedfish.util.UberTimer;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * a tablet is a wrapper of skiplist and a composing piece of memtable
 *  
 * @author wgu0
 */
public final class MemTablet implements ConsoleHelper, Recycable, Closeable, LogSpan {
    static Logger _log = UberUtil.getThisLogger();
    static Pattern _ptn = Pattern.compile("^([0-9a-fA-F]{8})-([0-9a-fA-F]{8})\\.tbl$");
    static final VariableLengthLongComparator _comp = new VariableLengthLongComparator();

    final static long SIG = 0x73746e61;
    final static byte VERSION = 0;
    public final static int HEADER_SIZE = 0x100;
    final static int OFFSET_SIG = 0;
    final static int OFFSET_CARBONFROZEN = 8;
    final static int OFFSET_DELETE_MARK = 9;
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
    
    BluntHeap heap;
    int size;
    Humpback humpback;
    private AtomicInteger gate = new AtomicInteger();
    private AtomicLong casRetries = new AtomicLong();
    private AtomicLong lockwaits = new AtomicLong();
    private AtomicLong errorConcurrencies = new AtomicLong();
    private boolean isMutable = true;
    FishSkipList slist;
    long base;
    TrxMan trxman;
    File file;
    MemoryMappedFile mmap;
    AtomicLong startTrx = new AtomicLong(Long.MIN_VALUE);
    AtomicLong endTrx = new AtomicLong(0);
    AtomicLong spStart = new AtomicLong();
    AtomicLong spEnd = new AtomicLong();
    int tableId;
    int tabletId;
    StorageTable mtable;
    SpaceManager sm;

    public final static class ListNode {
        private final static int OFFSET_SPACE_POINTER = 0;
        private final static int OFFSET_TRX_VERSION = 8;
        private final static int OFFSET_NEXT = 0x10; 
        private final static int OFFSET_TYPE = 0x14; 
        private final static int OFFSET_MISC = 0x15; 
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
        
        public static ListNode allocIndex(BluntHeap heap, long version, long pRowKey, int next) {
            int keySize = (pRowKey != 0) ? KeyBytes.getRawSize(pRowKey) : 0;
            int offset = heap.allocOffset(OFFSET_END + keySize);
            ListNode node = new ListNode(heap.getAddress(0), offset);
            node.setVersion(version);
            node.setSpacePointer(Long.MIN_VALUE);
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
    
    public static class Scanner extends ScanResult {
        MemTablet tablet;
        SkipListScanner upstream;
        long version;
        long trxid;
        long base;
        TrxMan trxman;
        private int oNext;
        private long spNextRow;
        long spfilterStart;
        long spfilterEnd;
        private boolean eof = false;
        
        
        @Override
        public long getVersion() {
            ListNode node = new ListNode(base, oNext);
            long result = node.getVersion();
            if (result < -10) {
                result = this.trxman.getTimestamp(result);
            }
            return result;
        }
        
        @Override
        public long getRowPointer() {
            if (this.spNextRow <= 1) {
                return this.spNextRow;
            }
            else {
                return this.tablet.sm.toMemory(this.spNextRow);
            }
        }

        public long getLogSpacePointer() {
            return this.spNextRow;
        }
        
        @Override
        public long getKeyPointer() {
            return this.upstream.getKeyPointer(); 
        }
        
        @Override
        public long getIndexRowKeyPointer() {
            return ListNode.getRowKeyAddress(base, oNext);
        }
        
        @Override
        public void rewind() {
            this.upstream.rewind();
            this.oNext = 0;
        }
        
        @Override
        public boolean eof() {
            if (this.eof) {
                return true;
            }
            return this.upstream.eof();
        }
        
        @Override
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
                    long sp = 0;
                    for (;;) {
                        sp = node.getSpacePointer();
                        // racing condition
                        if (sp != Long.MIN_VALUE) {
                            break;
                        }
                    }
                    if (this.spfilterStart != 0) {
                        if ((sp < this.spfilterStart) || (sp > this.spfilterEnd)) {
                            continue;
                        }
                    }
                    if (!node.isDeleted()) {
                        this.spNextRow = sp;
                    }
                    else {
                        this.spNextRow = 1;
                    }
                    this.oNext = oNode;
                    return true;
                }
            }
        }

        @Override
        public long getIndexSuffix() {
            long pKey = getKeyPointer();
            if (pKey == 0) {
                return 0;
            }
            long suffix = KeyBytes.create(pKey).getSuffix();
            return suffix;
        }

        @Override
        public byte getMisc() {
            ListNode node = new ListNode(base, oNext);
            return node.getMisc();
        }

        @Override
        public void close() {
            this.eof = true;
        }

        @Override
        public String toString() {
            String result = this.tablet.getRowLocation(this.oNext, this.spNextRow, getKeyPointer());
            return result;
        }
        
    }
    
    public MemTablet(File file) {
        this.file = file;
        this.tableId = getTableId(file);
        this.tabletId = getTabletId(file);
        this.heap = new BluntHeap(this.base, 0);
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
    MemTablet(File ns, int tableId, int tabletId, int size) 
    throws IOException {
        this(new File(ns, getFileName(tableId, tabletId)));
        this.tableId = tableId;
        this.tabletId = tabletId;
        this.file = getFile(ns, tableId, tabletId);
        this.size = size;
        
    }
    
    void setTransactionManager(TrxMan value) {
        this.trxman = value;
    }
    
    void setSpaceManager(SpaceManager value) {
        this.sm = value;
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
        long sp = getsp(trxid, version, pKey, missing);
        return (sp < 10) ? sp : this.sm.toMemory(sp);
    }
    
    long getsp(long trxid, long version, long pKey, long missing) {
        long pHead = this.slist.get(pKey);
        if (pHead == 0) {
            return 0;
        }
        int versionNode = getVersionNode(this.base, this.trxman, pHead, trxid, version);
        if (versionNode == 0) {
            return missing;
        }
        ListNode node = new ListNode(this.base, versionNode);
        if (node.isDeleted()) {
            return 1;
        }
        else {
            for (;;) {
                long result = node.getSpacePointer();
                // racing condition
                if (result != Long.MIN_VALUE) {
                    return result;
                }
            }
        }
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
            long spRow = 0;
            for (;;) {
                spRow = node.getSpacePointer();
                // racing condition
                if (spRow != Long.MIN_VALUE) {
                    break;
                }
            }
            long rowVersion = node.getVersion();
            if (rowVersion < 0) {
                rowVersion = this.trxman.getTimestamp(rowVersion);
            }
            keeper.pRow = this.sm.toMemory(spRow);
            keeper.version = rowVersion;
            if (!Row.checkSignature(keeper.pRow)) {
                String msg = String.format("invalid row %s", getRowLocation(node.getOffset(), spRow, pKey));
                throw new IllegalArgumentException(msg);
            }
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
        int oVersion = getVersionNode(this.base, this.trxman, pHead, trxid, version);
        if (oVersion == 0) {
            return null;
        }
        ListNode node = new ListNode(this.base, oVersion);
        return node;
    }
    
    Scanner scanAll() {
        SkipListScanner upstream = null; 
        upstream = this.slist.scan(0, true, 0, true);
        if (upstream == null) {
            return null;
        }
        Scanner scanner = new Scanner();
        scanner.tablet = this;
        scanner.upstream = upstream;
        scanner.base = base;
        scanner.trxman = this.trxman;
        scanner.version = Long.MAX_VALUE;
        scanner.trxid = 0;
        return scanner;
    }
    
    Scanner scanDelta(long spStart, long spEnd) {
        LongLong span = getLogSpan();
        if (span == null) {
            return null;
        }
        if ((spStart > span.y) || (spEnd < span.x)) {
            return null;
        }
        SkipListScanner upstream = null; 
        upstream = this.slist.scan(0, true, 0, true);
        if (upstream == null) {
            return null;
        }
        Scanner scanner = new Scanner();
        scanner.tablet = this;
        scanner.upstream = upstream;
        scanner.base = base;
        scanner.trxman = this.trxman;
        scanner.version = Long.MAX_VALUE;
        scanner.trxid = 0;
        scanner.spfilterStart = spStart;
        scanner.spfilterEnd = spEnd;
        return scanner;
    }
    
    Scanner scan(long trxid, long version, long pKeyStart, long pKeyEnd, long options) {
        boolean includeStart = ScanOptions.includeStart(options);
        boolean includeEnd = ScanOptions.includeEnd(options);
        boolean isAscending = ScanOptions.isAscending(options);
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
        scanner.trxman = this.trxman;
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
     * @return Long.MIN_VALUE when there is no pending data
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
        long now = this.trxman.getNewVersion();
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

    public boolean isClosed() {
        return this.mmap == null;
    }
    
    protected int getRowStateForWrite(
            long pKey, 
            int oHead, 
            long trxid, 
            long trxts, 
            Collection<MemTablet> pastTablets) {
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
            Collection<MemTablet> pastTablets) {
        // check history tablets
        
        for (MemTablet i:pastTablets) {
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
        
        // check minke
        
        StorageTable mtable = getMinkeTable();
        if (mtable != null) {
            boolean exist = mtable.exist(pKey);
            return exist ? RowState.EXIST : RowState.NONEXIST;
        }
        
        return RowState.NONEXIST;
    }
    
    private StorageTable getMinkeTable() {
        return this.mtable;
    }

    private int getRowStateForWrite(int oHead, long trxid, long compare) {
        for (ListNode i=ListNode.create(this.base, oHead); i!=null; i=i.getNextNode()) {
            long currentRawVersion;
            long currentVersion;
            currentRawVersion = i.getVersion();
            
            // realize the version if this is a trxid
            
            if (currentRawVersion < -10) {
                if (this.trxman != null) {
                    currentVersion = this.trxman.getTimestamp(currentRawVersion);
                }
                else {
                    // happens extremely rarely when trxman crashes
                    currentVersion = TrxMan.MARK_ROLLED_BACK; 
                }
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
        if (!isClosed()) {
            if (isMutable && !isCarbonfrozen()) {
                setDeleted(true);
                setCarbonfrozen(true);
            }
        }
        close();
        _log.debug("deleting file {} ...", this.file);
        HumpbackUtil.deleteHumpbackFile(this.file);
    }

    public long getBaseAddress() {
        return this.base;
    }

    @Override
    public LongLong getLogSpan() {
        if (this.spStart.get() == Long.MAX_VALUE) {
            return null;
        }
        LongLong result = new LongLong(this.spStart.get(), this.spEnd.get());
        return result;
    }
    
    MemTablet(Humpback humpback, File ns, int tableId, int tabletId, int size) throws IOException {
        this(ns, tableId, tabletId, size);
        this.humpback = humpback;
        if (humpback.getStorageEngine() != null) {
            this.mtable = humpback.getStorageEngine().getTable(tableId);
        }
        setTransactionManager(humpback.trxMan);
        setSpaceManager(humpback.getSpaceManager());
    }
    
    void open() throws IOException {
        if (this.isMutable) {
            openMutable();
        }
        else {
            openImmutable();
        }
    }
    
    private void openMutable() throws IOException {
        // creating new file
        
        _log.debug("creating new tablet {} ...", file);
        this.mmap = new MemoryMappedFile(file, size, "rw");
        this.base = this.mmap.getAddress();
        this.heap = new BluntHeap(this.base, size);
        this.heap.alloc(HEADER_SIZE);
        initFile();
        
        // init variables
        
        setCarbonfrozen(false);
        this.spStart.set(Long.MAX_VALUE);
        this.slist = FishSkipList.alloc(heap, _comp);
    }

    private void openImmutable() throws IOException {
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
        this.spStart.set(Unsafe.getLong(this.mmap.getAddress() + OFFSET_START_ROW));
        this.spEnd.set(Unsafe.getLong(this.mmap.getAddress() + OFFSET_END_ROW));
    }

    void setMutable(boolean value) {
        if (!isClosed()) {
            throw new IllegalArgumentException();
        }
        this.isMutable = value;
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
        this.spStart.set(Long.MAX_VALUE);
        this.spEnd.set(Long.MIN_VALUE);
    }
    
    /**
     * 
     * @param version
     * @param pKey
     * @param row null if this is mark-delete
     * @return
     */
    HumpbackError put(VaporizingRow row, Collection<MemTablet> past, int timeout) {
        ensureMutable();
        try {
            UberTimer timer = new UberTimer(timeout);
            this.gate.incrementAndGet();
            long version = row.getVersion();
            long pKey = row.getKeyAddress();
            for (;;) {
                long pHead = this.slist.put(pKey);
                int oHeadValue = Unsafe.getIntVolatile(pHead);
                int rowState = getRowStateForWrite(pKey, oHeadValue, version, 0, past);
                HumpbackError error = check(pKey, rowState, false, false);
                if (error == HumpbackError.SUCCESS) {
                    ListNode node = alloc(version, Long.MIN_VALUE, oHeadValue);
                    if (!casHead(pHead, oHeadValue, node.getOffset())) {
                        this.casRetries.incrementAndGet();
                        continue;
                    }
                    long spRow = 0;
                    if ((rowState == RowState.EXIST) || (rowState == RowState.EXIST_AND_LOCKED)) {
                        spRow = this.humpback.getGobbler().logUpdate(row, this.tableId);
                        node.setSpacePointer(spRow);
                    }
                    else if ((rowState == RowState.NONEXIST) || (rowState == RowState.TOMBSTONE)) {
                        spRow = this.humpback.getGobbler().logInsert(row, this.tableId);
                        node.setSpacePointer(spRow);
                    }
                    else {
                        throw new IllegalArgumentException(String.valueOf(rowState));
                    }
                    trackTrxId(version, node.getOffset());
                    trackSp(spRow - Gobbler.PutEntry.getHeaderSize());
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
    
    /**
     * recover an INSERT/UPDATE from the log
     * @param pKey
     * @param version
     * @param sp
     * @param past
     * @return
     */
    HumpbackError recoverPut(long pKey, long version, long sp, Collection<MemTablet> past) {
        ensureMutable();
        try {
            this.gate.incrementAndGet();
            for (;;) {
                long pHead = this.slist.put(pKey);
                int oHeadValue = Unsafe.getIntVolatile(pHead);
                ListNode node = alloc(version, sp, oHeadValue);
                if (!casHead(pHead, oHeadValue, node.getOffset())) {
                    this.casRetries.incrementAndGet();
                    continue;
                }
                trackTrxId(version, node.getOffset());
                trackSp(sp - Gobbler.PutEntry.getHeaderSize());
                return HumpbackError.SUCCESS;
            }
        }
        finally {
            this.gate.decrementAndGet();
        }
    }    
    
    HumpbackError insert(VaporizingRow row, Collection<MemTablet> pastTablets, int timeout) {
        ensureMutable();
        try {
            this.gate.incrementAndGet();
            UberTimer timer = new UberTimer(timeout);
            long pKey = row.getKeyAddress();
            long version = row.getVersion();
            if (version == 0) {
                throw new IllegalArgumentException();
            }
            for (;;) {
                long pHead = this.slist.put(pKey);
                int oHeadValue = Unsafe.getIntVolatile(pHead);
                int rowState = getRowStateForWrite(pKey, oHeadValue, version, 0, pastTablets);
                HumpbackError error = check(pKey, rowState, false, true);
                if (error == HumpbackError.SUCCESS) {
                    ListNode node = alloc(version, Long.MIN_VALUE, oHeadValue);
                    if (!casHead(pHead, oHeadValue, node.getOffset())) {
                        this.casRetries.incrementAndGet();
                        continue;
                    }
                    long spRow = LatencyDetector.run(_log, "logInsert", ()->{
                        return this.humpback.getGobbler().logInsert(row, this.tableId);
                    });
                    node.setSpacePointer(spRow);
                    trackTrxId(version, node.getOffset());
                    trackSp(spRow - Gobbler.InsertEntry.getHeaderSize());
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
            Collection<MemTablet> pastTablets, 
            byte misc,
            int timeout) {
        if (version == 0) {
            throw new IllegalArgumentException();
        }
        ensureMutable();
        try {
            this.gate.incrementAndGet();
            UberTimer timer = new UberTimer(timeout);
            for (;;) {
                long pHead = this.slist.put(pIndexKey);
                int oHeadValue = Unsafe.getIntVolatile(pHead);
                int rowState = getRowStateForWrite(pIndexKey, oHeadValue, version, 0, pastTablets);
                HumpbackError error = check(pIndexKey, rowState, false, true);
                if (error == HumpbackError.SUCCESS) {
                    ListNode node = ListNode.allocIndex(heap, version, pRowKey, oHeadValue);
                    node.setMisc(misc);
                    if (!casHead(pHead, oHeadValue, node.getOffset())) {
                        this.casRetries.incrementAndGet();
                        continue;
                    }
                    long sp = this.humpback.getGobbler().logIndex(tableId, version, pIndexKey, pRowKey, misc);
                    node.setSpacePointer(sp);
                    trackTrxId(version, node.getOffset());
                    trackSp(sp - Gobbler.IndexEntry.getHeaderSize());
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
    
    HumpbackError recoverIndexInsert(long pIndexKey, 
            long version, 
            long pRowKey, 
            Collection<MemTablet> pastTablets,
            long sp,
            byte misc,
            int timeout) {
        if (version == 0) {
            throw new IllegalArgumentException();
        }
        ensureMutable();
        try {
            this.gate.incrementAndGet();
            for (;;) {
                long pHead = this.slist.put(pIndexKey);
                int oHeadValue = Unsafe.getIntVolatile(pHead);
                ListNode node = ListNode.allocIndex(heap, version, pRowKey, oHeadValue);
                node.setMisc(misc);
                if (!casHead(pHead, oHeadValue, node.getOffset())) {
                    this.casRetries.incrementAndGet();
                    continue;
                }
                node.setSpacePointer(sp);
                trackTrxId(version, node.getOffset());
                trackSp(sp - Gobbler.IndexEntry.getHeaderSize());
                return HumpbackError.SUCCESS;
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
    public HumpbackError lock(long trxid, long pKey, Collection<MemTablet> pastTablets, int timeout) {
        if (trxid == 0) {
            throw new IllegalArgumentException();
        }
        try {
            this.gate.incrementAndGet();
            UberTimer timer = new UberTimer(timeout);
            for (;;) {
                long pHead = this.slist.put(pKey);
                int oHeadValue = Unsafe.getIntVolatile(pHead);
                int rowState = getRowStateForWrite(pKey, oHeadValue, trxid, 0, pastTablets);
                HumpbackError error = check(pKey, rowState, true, false);
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
    
    HumpbackError update(VaporizingRow row, long compare, Collection<MemTablet> pastTablets, int timeout) {
        try {
            this.gate.incrementAndGet();
            UberTimer timer = new UberTimer(timeout);
            long pKey = row.getKeyAddress();
            long version = row.getVersion();
            for (;;) {
                long pHead = this.slist.put(pKey);
                int oHeadValue = Unsafe.getIntVolatile(pHead);
                int rowState = getRowStateForWrite(pKey, oHeadValue, version, compare, pastTablets);
                HumpbackError error = check(pKey, rowState, true, false);
                if (error == HumpbackError.SUCCESS) {
                    ListNode node = alloc(version, Long.MIN_VALUE, oHeadValue);
                    if (!casHead(pHead, oHeadValue, node.getOffset())) {
                        this.casRetries.incrementAndGet();
                        continue;
                    }
                    long spRow = this.humpback.getGobbler().logUpdate(row, this.tableId);
                    node.setSpacePointer(spRow);
                    trackTrxId(version, node.getOffset());
                    trackSp(spRow - Gobbler.UpdateEntry.getHeaderSize());
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
    
    HumpbackError delete(long pKey, long trxid, Collection<MemTablet> pastTablets, int timeout) {
        if (trxid == 0) {
            throw new IllegalArgumentException();
        }
        ensureMutable();
        try {
            this.gate.incrementAndGet();
            UberTimer timer = new UberTimer(timeout);
            for (;;) {
                long pHead = this.slist.put(pKey);
                int oHeadValue = Unsafe.getIntVolatile(pHead);
                int rowState = getRowStateForWrite(pKey, oHeadValue, trxid, 0, pastTablets);
                HumpbackError error = check(pKey, rowState, true, false);
                if (error == HumpbackError.SUCCESS) {
                    ListNode node = alloc(trxid, Long.MIN_VALUE, oHeadValue);
                    node.setDeleted(true);
                    if (!casHead(pHead, oHeadValue, node.getOffset())) {
                        this.casRetries.incrementAndGet();
                        continue;
                    }
                    int length = KeyBytes.getRawSize(pKey);
                    long sprow = this.humpback.getGobbler().logDelete(trxid, this.tableId, pKey, length);
                    node.setSpacePointer(sprow);
                    trackTrxId(trxid, node.getOffset());
                    trackSp(sprow - Gobbler.DeleteEntry.getHeaderSize());
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

    public HumpbackError deleteRow(long pRow, long trxid, ConcurrentLinkedList<MemTablet> tablets, int timeout) {
        if (trxid == 0) {
            throw new IllegalArgumentException();
        }
        ensureMutable();
        try {
            long pKey = Row.getKeyAddress(pRow);
            this.gate.incrementAndGet();
            UberTimer timer = new UberTimer(timeout);
            for (;;) {
                long pHead = this.slist.put(pKey);
                int oHeadValue = Unsafe.getIntVolatile(pHead);
                int rowState = getRowStateForWrite(pKey, oHeadValue, trxid, 0, tablets);
                HumpbackError error = check(pKey, rowState, true, false);
                if (error == HumpbackError.SUCCESS) {
                    ListNode node = alloc(trxid, Long.MIN_VALUE, oHeadValue);
                    node.setDeleted(true);
                    if (!casHead(pHead, oHeadValue, node.getOffset())) {
                        this.casRetries.incrementAndGet();
                        continue;
                    }
                    long sprow = this.humpback.getGobbler().logDeleteRow(trxid, this.tableId, pRow);
                    node.setSpacePointer(sprow);
                    trackTrxId(trxid, node.getOffset());
                    trackSp(sprow - Gobbler.DeleteRowEntry.getHeaderSize());
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
    
    /**
     * recover a DELETE from the log
     * 
     * @param pKey
     * @param trxid
     * @param pastTablets
     * @param sprow
     * @return
     */
    HumpbackError recoverDelete(long pKey, long trxid, Collection<MemTablet> pastTablets, long sprow) {
        if (trxid == 0) {
            throw new IllegalArgumentException();
        }
        ensureMutable();
        try {
            this.gate.incrementAndGet();
            for (;;) {
                long pHead = this.slist.put(pKey);
                int oHeadValue = Unsafe.getIntVolatile(pHead);
                ListNode node = alloc(trxid, sprow, oHeadValue);
                node.setDeleted(true);
                if (!casHead(pHead, oHeadValue, node.getOffset())) {
                    this.casRetries.incrementAndGet();
                    continue;
                }
                trackTrxId(trxid, node.getOffset());
                trackSp(sprow - Gobbler.DeleteEntry.getHeaderSize());
                return HumpbackError.SUCCESS;
            }
        }
        finally {
            this.gate.decrementAndGet();
        }
    }
    
    private HumpbackError check(long pKey, int rowState, boolean mustExist, boolean mustNotExist) {
        if (rowState == HumpbackError.CONCURRENT_UPDATE.ordinal()) {
            return HumpbackError.CONCURRENT_UPDATE;
        }
        else if (rowState == HumpbackError.LOCK_COMPETITION.ordinal()) {
            return HumpbackError.LOCK_COMPETITION;
        }
        if (mustExist) {
            if ((rowState != RowState.EXIST) && (rowState != RowState.EXIST_AND_LOCKED)) {
                return HumpbackError.MISSING;
            }
        }
        if (mustNotExist) {
            if ((rowState != RowState.NONEXIST) && (rowState != RowState.TOMBSTONE)) {
                return HumpbackError.EXISTS;
            }
        }
        return HumpbackError.SUCCESS;
    }
    
    private String getKeySpec(long pKey) {
        String result = String.valueOf(this.tableId) + ":" + KeyBytes.toString(pKey);
        return result;
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
    synchronized void render(long lastClosed) {
        if (isCarbonfrozen()) {
            return;
        }
        
        // check if there is any data available for rendering
        
        long startTrxId = this.startTrx.get();
        if ((startTrxId == Long.MIN_VALUE) || (startTrxId < lastClosed)) {
            return;
        }

        // reset startTrx and endTrx
        
        _log.trace("rendering {} from trxid {} ...", this.file, lastClosed);
        this.startTrx.set(Long.MIN_VALUE);
        this.endTrx.set(0);
        AtomicLong minTrxId = new AtomicLong(0);
        AtomicLong maxTrxId = new AtomicLong(Long.MIN_VALUE);
        
        // scan all values and realize trx id
        
        AtomicInteger count = new AtomicInteger();
        long now = this.trxman.getCurrentVersion();
        scanAllVersion(offset -> {
            // track start sprow and end sprow
            
            ListNode node = new ListNode(this.base, offset);
            long version = node.getVersion();

            // skip if this node is already rendered
            
            if (version >= -10) {
                return true;
            }
            
            // render it
            
            if (version >= lastClosed) {
                long trxts = this.trxman.getTimestamp(version);
                if (trxts >= -10) {
                    if (trxts <= now) {
                        node.setVersion(trxts);
                        count.incrementAndGet();
                        return true;
                    }
                    else {
                        _log.warn("trxts is newer than current timestamp: ", trxts);
                    }
                }
                else {
                    node.setVersion(MARK_ROLLED_BACK);
                    _log.warn("trxts not found for trxid: {} sp {} is set to rollback", 
                            version,
                            hex(node.getSpacePointer()));
                }
            }
            else {
                AtomicUtil.max(maxTrxId, version);
                AtomicUtil.min(minTrxId, version);
            }
            return true;
        });
        
        // update 
        
        AtomicUtil.max(this.startTrx, maxTrxId.get());
        AtomicUtil.min(this.endTrx, minTrxId.get());
        _log.trace("rendering {} ended with startTrxId={} count={}", this.file, this.startTrx.get(), count);
    }

    /**
     * write the tablet to storage
     * 
     * @param oldestTrxid oldest active trxid in the system
     * @param force forcing carbonfreeze even if the tablet is not filled up
     * @return true if success. false if there are pending transactions or already carbonfrozen 
     * @return
     */
    public synchronized boolean carbonfreeze(long oldestTrxid, boolean force) {
        if (isCarbonfrozen()) {
            return false;
        }
        
        if (this.mmap.isReadOnly()) {
            throw new CodingError(this.toString());
        }
        
        // must be frozen. cant freeze a moving wheel 
        
        if (!isFrozen()) {
            if (!force) {
                return false;
            }
            this.heap.freeze();
        }
        
        // wait until there is no write operations
        
        UberTimer timer = new UberTimer(1000);
        while (this.gate.get() > 1) {
            if (timer.isExpired()) {
                return false;
            }
        }

        // if the tablet trx window is beyond the oldest active trx
        
        if (getEndTrxId() < oldestTrxid) {
            return false;
        }
        
        render(oldestTrxid);
        Unsafe.putLong(this.mmap.getAddress() + OFFSET_START_ROW, this.spStart.get());
        Unsafe.putLong(this.mmap.getAddress() + OFFSET_END_ROW, this.spEnd.get());
        setCarbonfrozen(true);
        
        // write to disk
        
        if (this.mmap.isReadOnly()) {
            throw new CodingError();
        }
        this.mmap.force();
        _log.debug("{} is carbonfrozen {} - {}", this.file.getName(), hex(this.spStart.get()), hex(this.spEnd.get()));
        
        return true;
    }
    
    @Override
    public synchronized void close() {
        if (isClosed()) {
            return;
        }
        
        boolean isReadOnly = this.mmap.isReadOnly(); 

        this.mmap.close();
        this.mmap = null;
        this.slist = null;
        this.base = 0;
        
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

    @Override
    public void recycle() {
        drop();
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
    
    void setCarbonfrozen(boolean b) {
        Unsafe.putByteVolatile(this.mmap.getAddress() + OFFSET_CARBONFROZEN, (byte)(b ? 1 : 0));
    }
    
    /**
     * 
     * @return true if the tablet is carbonzied
     */
    public boolean isCarbonfrozen() {
        byte result = Unsafe.getByteVolatile(this.mmap.getAddress() + OFFSET_CARBONFROZEN);
        return result != 0;
    }
    
    private void setDeleted(boolean value) {
        Unsafe.putByteVolatile(this.mmap.getAddress() + OFFSET_DELETE_MARK, (byte)(value ? 1 : 0));
    }

    boolean isDeleted() {
        byte result = Unsafe.getByteVolatile(this.mmap.getAddress() + OFFSET_DELETE_MARK);
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
	    if (Thread.interrupted()) {
	        throw new HumpbackException("thread killed");
	    }
		if (error == HumpbackError.LOCK_COMPETITION) {
			this.lockwaits.incrementAndGet();
			if (!timer.isExpired()) {
				try {
					ListNode node = new ListNode(this.base, oHeadValue);
					registerLock(trxid, node.getVersion(), pKey, oHeadValue);
					Thread.sleep(0, 10000);
				}
				catch (InterruptedException e) {
		            throw new HumpbackException("thread killed");
				}
				return true;
			}
			else {
                ListNode node = new ListNode(this.base, oHeadValue);
                throw new HumpbackException("failed to acquire lock {} oHeadValue={} timeout={} trxid={} node={}",
                        getKeySpec(pKey),
                        hex(oHeadValue),
                        timer.getTimeOut(),
                        trxid,
                        node.toString());
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

    /**
     * track space pointer
     * @param sp
     */
    private void trackSp(long value) {
        if (value == 0) {
            return;
        }
        AtomicUtil.min(this.spStart, value);
        AtomicUtil.max(this.spEnd, value);
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
    public void merge(MemTablet x, MemTablet y) {
        if (x.getTabletId() < y.getTabletId()) {
            // x must be newer than y in order to must sure version of the same key is following descending order
            MemTablet t = x;
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

    void setMinkeTable(StorageTable value) {
        this.mtable = value;
    }
    
    private void ensureMutable() {
        if (!this.isMutable) {
            throw new OutOfHeapMemory();
        }
    }

    public boolean isMutable() {
        return this.isMutable;
    }

    int getFileSize() {
        return this.size;
    }

    public String getLocation(long trxid, long version, long pKey) {
        ListNode node = getListNode(trxid, version, pKey);
        if (node == null) {
            return null;
        }
        long sp = node.getSpacePointer();
        return getRowLocation(node.getOffset(), sp, pKey);
    }

    private String getRowLocation(int offset, long sp, long pKey) {
        String result = this.toString() + ":" + hex(offset);
        result += "->" + this.sm.getLocation(sp);
        result += " " + KeyBytes.toString(pKey);
        return result;
    }

    public boolean traceIo(long pKey, List<FileOffset> lines) {
        long pOffset = this.slist.traceIo(pKey, this.file, this.base, lines);
        if (pOffset == 0) {
            return false;
        }
        int head = Unsafe.getInt(pOffset);
        if (head == 0) {
            return false;
        }
        for (ListNode i=ListNode.create(base, head); i!=null; i=i.getNextNode()) {
            lines.add(new FileOffset(this.file, i.getOffset(), "version"));
            long versionInList = i.getVersion();
            if (versionInList < 0) {
                continue;
            }
            long sp = i.getSpacePointer();
            
            lines.add(this.sm.getFileOffset(sp).setNote("row"));
            return true;
        }
        return false;
    }
}
