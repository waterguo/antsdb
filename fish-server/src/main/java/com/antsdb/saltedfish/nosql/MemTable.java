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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.FileOffset;
import com.antsdb.saltedfish.cpp.OutOfHeapException;
import com.antsdb.saltedfish.cpp.VariableLengthLongComparator;
import com.antsdb.saltedfish.nosql.ConcurrentLinkedList.Node;
import com.antsdb.saltedfish.util.LongLong;
import com.antsdb.saltedfish.util.ScalableData;
import com.antsdb.saltedfish.util.UberUtil;

public final class MemTable extends ScalableData implements LogSpan {
    static final Logger _log = UberUtil.getThisLogger();

    final static int MAGIC = 0x0211;
    final static int VERSION = 1;
    final static int INITIAL_FILE_SIZE = 16 * 1024;

    static final VariableLengthLongComparator _comp = new VariableLengthLongComparator();

    Humpback owner;
    volatile MemTablet tablet;
    private int tabletSize;
    private boolean isClosed = true;
    private Map<Integer, MemTablet> retired = Collections.synchronizedMap(new HashMap<>());
    private boolean isMutable = true;
    private boolean isRecoveryMode = false;
    int tableId;
    ConcurrentLinkedList<MemTablet> tablets = new ConcurrentLinkedList<>();
    File ns;
    private SpaceManager spaceman;
    private StorageEngine storage;
    private volatile int ticket;
    private int nextTabletId = 0;
    
    static class Scanner implements RowIterator {
        List<ScanResult> upstreams = new ArrayList<>();
        long counter = 0;
        ScanResult lastFetched = null;
        long pRow = 0;
        long version = 0;
        boolean eof = false;
        SpaceManager spaceman;
        boolean isAscending = true;
        
        @Override
        public boolean next() {
            return next_();
        }
        
        public boolean next_() {
            if (this.eof) {
                return false;
            }
            if (Thread.interrupted()) {
                throw new HumpbackException("thread killed");
            }
            
            // 1 step forward
            
            if (lastFetched != null) {
                lastFetched.next();
            }
            else {
                for (ScanResult i:upstreams) {
                    i.next();
                }
            }
            
            // find the smallest key in ascending order. or the greatest in descending order 
            
            long pKey = 0;
            for (ScanResult i:upstreams) {
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
            int result = MemTable._comp.compare(pKeyX, pKeyY);
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
            for (ScanResult i:upstreams) {
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
            long pRow = getRowPointer();
            if (pRow == 0) {
                return null;
            }
            Row row = Row.fromMemoryPointer(pRow, getVersion());
            return row;
        }

        @Override
        public long getRowKeyPointer() {
            long result = this.lastFetched.getIndexRowKeyPointer();
            return result;
        }

        @Override
        public void close() {
            for (ScanResult i:this.upstreams) {
                i.close();
            }
        }

        @Override
        public long getKeyPointer() {
            return this.lastFetched.getKeyPointer();
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

        @Override
        public String toString() {
            if (this.lastFetched == null) {
                return "null";
            }
            return this.lastFetched.toString();
        }
        
    }
    
    public MemTable(Humpback owner, File ns, int tableId, int tabletSize) {
        this(owner.getSpaceManager(), owner.getStorageEngine(), tableId);
        this.ns = ns;
        this.tabletSize = tabletSize;
        this.owner = owner;
        this.tableId = tableId;
        this.ns = ns;
    }
    
    public MemTable(SpaceManager spaceman, StorageEngine minke, File ns, int tableId) {
        this(spaceman, minke, tableId);
        this.ns = ns;
    }
    
    protected MemTable(SpaceManager spaceman, StorageEngine minke, int tableId) {
        this.spaceman = spaceman;
        this.tableId = tableId;
        this.storage = minke;
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
    public long get(long trxid, long version, long pKey, long options) {
        for (MemTablet ii:this.tablets) {
            long pRow = ii.get(trxid, version, pKey, 0);
            if (pRow != 0) {
                return Row.isTombStone(pRow) ? 0 : pRow;
            }
        }
        StorageTable mtable = getStorageTable();
        if (mtable == null) {
            return 0;
        }
        long pResult = mtable.get(pKey, options);
        return pResult;
    }
    
    public Row getRow(long trxid, long version, long pKey, long options) {
        RowKeeper keeper = new RowKeeper();
        for (MemTablet ii:this.tablets) {
            int result = ii.getRow(keeper, trxid, version, pKey);
            if (result == 1) {
                return Row.fromMemoryPointer(keeper.pRow, keeper.version);
            }
            else if (result == -1) {
                // tomb stone
                return null;
            }
        }
        StorageTable mtable = getStorageTable();
        if (mtable == null) {
            return null;
        }
        long pResult = mtable.get(pKey, options);
        if (Row.isTombStone(pResult)) {
            return null;
        }
        return Row.fromMemoryPointer(pResult, 0);
    }
    
    public long getIndex(long trxid, long version, long pKey) {
        for (MemTablet ii:this.tablets) {
            long pRowKey = ii.getIndex(trxid, version, pKey);
            if (pRowKey != 0) {
                return Row.isTombStone(pRowKey) ? 0 : pRowKey;
            }
        }
        StorageTable mtable = getStorageTable();
        if (mtable == null) {
            return 0;
        }
        long pResult = mtable.getIndex(pKey);
        return pResult;
    }
    
    /**
     * scan for data that only kept in memory table
     * @param inclusive 
     * @param spStart 
     * 
     * @return
     */
    public RowIterator scanDelta(long spStart, long spEnd) {
        Scanner scanner = new Scanner();
        scanner.isAscending = true;
        scanner.spaceman = this.spaceman;
        for (MemTablet i:this.tablets) {
            MemTablet.Scanner upstream;
            upstream = i.scanDelta(spStart, spEnd);
            if (upstream != null) {
                scanner.upstreams.add(upstream);
            }
        }
        return scanner;
    }

    public Scanner scan(long trxid, long version,long pKeyStart, long pKeyEnd, long options) {
        Scanner scanner = new Scanner();
        scanner.isAscending = ScanOptions.isAscending(options);
        scanner.spaceman = this.spaceman;
        for (MemTablet i:this.tablets) {
            MemTablet.Scanner upstream;
            upstream = i.scan(trxid, version, pKeyStart, pKeyEnd, options);
            if (upstream != null) {
                scanner.upstreams.add(upstream);
            }
        }
        StorageTable mtable = getStorageTable();
        if (mtable != null) {
            ScanResult upstream = mtable.scan(pKeyStart, pKeyEnd, options);
            if (upstream != null) {
                scanner.upstreams.add(upstream);
            }
        }
        return scanner;
    }
    
    long size() {
        long size = 0;
        for (MemTablet tablet:this.tablets) {
            size += tablet.size();
        }
        return size;
    }

    public ConcurrentLinkedList<MemTablet> getTabletsReadOnly() {
        return this.tablets;
    }

    boolean validate() {
        boolean result = true;
        for (MemTablet i:this.tablets) {
            result = result && i.validate();
        }
        return result;
    }

    /**
     * 
     * @return Long.MIN_VALUE when there is pending data
     */
    public long getStartTrxId() {
        long startTrxId = Long.MIN_VALUE;
        for (MemTablet tablet:this.tablets) {
            long tabletStartTrxId = tablet.getStartTrxId();
            if (tabletStartTrxId == 0) {
                continue;
            }
            startTrxId = Math.max(startTrxId, tabletStartTrxId);
        }
        return startTrxId;
    }

    protected StorageTable getStorageTable() {
        if (this.storage == null) {
            return null;
        }
        return this.storage.getTable(tableId);
    }

    @SuppressWarnings("unchecked")
    @Override
    public LongLong getLogSpan() {
        LongLong result = LogSpan.union((Collection<LogSpan>)(Collection<?>)this.tablets);
        return result;
    }
    /**
     * open the mem table
     * 
     * @param deleteCorruptedFile delete tablets not closed properly in last shutdown if true
     * @throws IOException 
     */
    public void open() throws IOException {
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
            if (!i.getName().endsWith(".tbl")) {
                continue;
            }
            if (i.length() == 0) {
                continue;
            }
            MemTablet tablet = new MemTablet(i);
            tablet.setMinkeTable(getStorageTable());
            tablet.setMutable(false);
            tablet.setSpaceManager(this.owner.spaceman);
            tablet.open();
            if (isRecoveryMode && !tablet.isCarbonfrozen()) {
                _log.warn("{} is not properly closed. deleting ... {}", i, i.exists());
                tablet.close();
                if (!i.delete()) {
                    throw new HumpbackException("unable to delete file " + i.toString());
                }
                continue;
            }
            if (!tablet.isCarbonfrozen()) {
                _log.error("{} mounted read-only but not carbonfrozen", i);
            }
            this.tablets.addLast(tablet);
        }
        
        // set next tablet id
        
        if (this.tablets.size() != 0) {
            this.nextTabletId = this.tablets.getFirst().getTabletId() + 1;
        }
        
        // done
        
        this.isClosed = false;
    }

    public HumpbackError insertIndex(
            HumpbackSession hsession, 
            long trxid, 
            long pIndexKey, 
            long pRowKey, 
            byte misc, 
            int timeout) {
        for (;;) {
            MemTablet current = this.tablet;
            if (current == null) {
                grow(this.ticket);
                continue;
            }
            try {
                return current.insertIndex(hsession, pIndexKey, trxid, pRowKey, this.tablets, misc, timeout);
            }
            catch (OutOfHeapException x) {
                grow(this.ticket);
            }
        }
    }
    
    public HumpbackError insert(HumpbackSession hsession, VaporizingRow row, int timeout) {
        for (;;) {
            MemTablet current = this.tablet;
            if (current == null) {
                grow(this.ticket);
                continue;
            }
            try {
                return current.insert(hsession, row, this.tablets, timeout);
            }
            catch (OutOfHeapException x) {
                grow(this.ticket);
            }
        }
    }
    
    public HumpbackError update(HumpbackSession hsession, VaporizingRow row, long oldVersion, int timeout) {
        for (;;) {
            MemTablet current = this.tablet;
            if (current == null) {
                grow(this.ticket);
                continue;
            }
            try {
                return current.update(hsession, row, oldVersion, this.tablets, timeout);
            }
            catch (OutOfHeapException x) {
                grow(this.ticket);
            }
        }
    }
    
    public HumpbackError delete(HumpbackSession hsession, long trxid, long pKey, int timeout) {
        for (;;) {
            MemTablet current = this.tablet;
            if (current == null) {
                grow(this.ticket);
                continue;
            }
            try {
                return current.delete(hsession, pKey, trxid, this.tablets, timeout);
            }
            catch (OutOfHeapException x) {
                grow(this.ticket);
            }
        }
    }
    
    public HumpbackError deleteRow(HumpbackSession hsession, long trxid, long pRow, int timeout) {
        for (;;) {
            MemTablet current = this.tablet;
            if (current == null) {
                grow(this.ticket);
                continue;
            }
            try {
                return current.deleteRow(hsession, pRow, trxid, this.tablets, timeout);
            }
            catch (OutOfHeapException x) {
                grow(this.ticket);
            }
        }
    }
    
    public HumpbackError put(HumpbackSession hsession, VaporizingRow row, int timeout) {
        for (;;) {
            MemTablet current = this.tablet;
            if (current == null) {
                grow(this.ticket);
                continue;
            }
            try {
                return current.put(hsession, row, this.tablets, timeout);
            }
            catch (OutOfHeapException x) {
                grow(this.ticket);
            }
        }
    }
    
    public HumpbackError recover(long lpLogEntry) {
        for (;;) {
            MemTablet current = this.tablet;
            if (current == null) {
                grow(this.ticket);
                continue;
            }
            try {
                return current.recover(lpLogEntry);
            }
            catch (OutOfHeapException x) {
                grow(this.ticket);
            }
        }
    }
    
    /**
     * it is only used by TestMemTableConcurrency
     * 
     * @deprecated
     */
    HumpbackError recoverPut(long trxid, long pKey, long spRow) {
        for (;;) {
            MemTablet current = this.tablet;
            if (current == null) {
                grow(this.ticket);
                continue;
            }
            try {
                return current.recoverPut(pKey, trxid, spRow, this.tablets);
            }
            catch (OutOfHeapException x) {
                grow(this.ticket);
            }
        }
    }
    
    public HumpbackError lock(long trxid, long pKey, int timeout) {
        if (this.tablet == null) {
            grow(this.ticket);
        }
        for (;;) {
            MemTablet current = this.tablet;
            try {
                return current.lock(trxid, pKey, this.tablets, timeout);
            }
            catch (OutOfHeapException x) {
                grow(this.ticket);
            }
        }
    }

    public void testEscape(VaporizingRow row) {
        if (tablet != null) {
            this.tablet.testEscape(row);
        }
    }

    public synchronized void close() {
        this.isClosed = true;
        ConcurrentLinkedList<MemTablet> list = new ConcurrentLinkedList<MemTablet>(this.tablets);
        this.tablets.clear();
        for (MemTablet i:list) {
            i.close();
        }
        this.tablet = null;
    }
    
    public synchronized void drop() {
        if (!this.isMutable) {
            throw new IllegalArgumentException();
        }
        this.isClosed = true;
        for (MemTablet i:getTablets()) {
            this.owner.gc.free(i);
        }
        this.tablets.clear();
        this.tablet = null;
    }

    @Override
    protected void extend(int requestTicket, int nextTicket) {
        if (!this.isMutable) {
            throw new IllegalArgumentException();
        }
        int tabletId = this.nextTabletId++;
        MemTablet newone = null;
        try {
            int filesize = (this.tablet == null) ? INITIAL_FILE_SIZE : this.tabletSize;
            newone = new MemTablet(owner, this.ns, this.tableId, tabletId, filesize);
            newone.setMinkeTable(getStorageTable());
            newone.setMutable(true);
            newone.setSpaceManager(this.owner.getSpaceManager());
            newone.setTransactionManager(this.owner.getTrxMan());
            newone.open();
        }
        catch (IOException x) {
            // clean up the file if we have problems opening it. this could happen during shutdown
            cleanup(newone);
            throw new HumpbackException(x);
        }
        this.tablet = newone;
        this.tablets.addFirst(newone);
        this.ticket = nextTicket;
    }
    
    private void cleanup(MemTablet tablet) {
        if (tablet == null) {
            return;
        }
        try {
            tablet.drop();
        }
        catch (Exception ignored) {}
    }

    public void render(long endTrxId) {
        for (MemTablet tablet:getTabletsReadOnly()) {
            if (!(tablet instanceof MemTablet)) {
                continue;
            }
            ((MemTablet)tablet).render(endTrxId);
        }
    }

    /**
     * carbonfreeze tablets 
     * 
     * @param oldestTrxid oldest active trxid in the system
     * @param force forcing carbonfreeze even if the tablet is not filled up
     * @return number of tablets carbonfrozen
     * @throws IOException
     */
    public synchronized int carbonfreeze(long oldestTrxid, boolean force) throws IOException {
        int count = 0;
        for (MemTablet i:this.tablets) {
            if (i.isMutable()) {
                MemTablet tablet = (MemTablet)i;
                if (tablet.carbonfreeze(oldestTrxid, force)) {
                    count++;
                }
            }
        }
        return count;
    }
    
    public synchronized int carbonfreezeIfPossible(long oldestTrxid) throws IOException {
        return carbonfreeze(oldestTrxid, false);
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
        
        ConcurrentLinkedList.Node<MemTablet> node = findCompactCandidate();
        if (node == null) {
            return;
        }
        
        // double check existence of new file
        
        MemTablet lead = node.data;
        int babyTabletId = lead.getTabletId()-1;
        File babyFile = MemTablet.getFile(ns, this.tableId, babyTabletId);
        if (babyFile.exists()) {
            _log.error("compact failed due to existence of file {}", babyFile);
            return;
        }
        
        // start compacting
        
        MemTablet x = node.next.data;
        MemTablet y = node.next.next.data;
        long size = getSizeOfNextTwoTablets(node) * 3 / 2;
        if (size >= Integer.MAX_VALUE) {
            // file is too big, give up
            return;
        }
        _log.debug("start merging {} and {} to {} ...", x.getTabletId(), y.getTabletId(),babyFile);
        MemTablet baby = new MemTablet(this.owner, this.ns, this.tableId, babyTabletId, (int)size);
        boolean success = false;
        try {
            ((MemTablet)baby).merge(node.next.data, node.next.next.data);
            ((MemTablet)baby).carbonfreeze(this.owner.getLastClosedTransactionId(), false);
            baby.close();
            baby = new MemTablet(babyFile);
            baby.setMutable(false);
            baby.setSpaceManager(this.owner.getSpaceManager());
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
        
        ConcurrentLinkedList.Node<MemTablet> babyNode = this.tablets.insert(node, baby);
        this.retired.put(x.getTabletId(), x);
        this.retired.put(y.getTabletId(), y);
        this.tablets.deleteNext(babyNode);
        this.tablets.deleteNext(babyNode);
        this.owner.getJobManager().schedule(5, TimeUnit.MINUTES, () -> {
            _log.debug("deleting {} due to compacting", x.getFile());
            x.drop();
            y.drop();
            this.retired.remove(x.getTabletId());
            this.retired.remove(y.getTabletId());
        });
    }
        
    private ConcurrentLinkedList.Node<MemTablet> findCompactCandidate() {
        // find the two consecutive tablets with smallest size
        
        long minSize = Integer.MAX_VALUE;
        ConcurrentLinkedList.Node<MemTablet> tabletWithMinSize = null;
        for (ConcurrentLinkedList.Node<MemTablet> i=this.tablets.getFirstNode(); i!=null; i=i.next) {
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

    private long getSizeOfNextTwoTablets(Node<MemTablet> node) {
        if (node.next == null) {
            return Long.MAX_VALUE; 
        }
        if (node.next.next == null) {
            return Long.MAX_VALUE;
        }
        return node.next.data.getFile().length() + node.next.next.data.getFile().length();
    }

    private boolean isNextTabletIdAvailable(Node<MemTablet> node) {
        if (node.next == null) {
            return false;
        }
        int mergedTabletId = node.data.getTabletId() - 1;
        MemTablet nextTablet = node.next.data;
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

    /**
     * free tabltes whose sp is before specified sp
     * 
     * @param sp
     */
    public void free(long sp) {
        List<MemTablet> junk = new ArrayList<>(); 
        for (MemTablet i:this.getTablets()) {
            if (!i.isCarbonfrozen()) {
                continue;
            }
            LongLong span = i.getLogSpan();
            if (span == null) {
                continue;
            }
            if (i.getLogSpan().y <= sp) {
                junk.add(i);
            }
        }
        for (MemTablet i:junk) {
            if (i == this.tablet) {
                this.tablet = null;
            }
            getTablets().remove(i);
            this.owner.gc.free(i);
            _log.debug("tablet {} is freed", i);
        }
    }

    public void setMutable(boolean value) {
        if (!this.isClosed) {
            throw new IllegalArgumentException();
        }
        this.isMutable = value;

    }
    
    /**
     * corrupted file will be deleted in recovery mode
     * 
     * @param value
     */
    public void setRecoveryMode(boolean value) {
        if (!this.isClosed) {
            throw new IllegalArgumentException();
        }
        this.isRecoveryMode = value;
    }

    public String getLocation(long trxid, long version, long pKey) {
        for (MemTablet ii:this.tablets) {
            String result = ii.getLocation(trxid, version, pKey);
            if (result != null) {
                return result;
            }
        }
        StorageTable mtable = getStorageTable();
        if (mtable == null) {
            return null;
        }
        String result = mtable.getLocation(pKey);
        return result;
    }

    public void traceIo(long pKey, List<FileOffset> lines) {
        for (MemTablet ii:this.tablets) {
            if (ii.traceIo(pKey, lines)) {
                return;
            }
        }
        StorageTable mtable = getStorageTable();
        if (mtable == null) {
            return;
        }
        mtable.traceIo(pKey, lines);
    }

}
