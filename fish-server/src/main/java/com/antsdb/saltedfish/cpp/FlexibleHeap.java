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
package com.antsdb.saltedfish.cpp;

import java.nio.ByteBuffer;

import com.antsdb.saltedfish.util.UberUtil;

/**
 * a heap that grows on demand
 * 
 * NOT THREAD SAFE
 * 
 * @author wgu0
 */
public class FlexibleHeap extends Heap implements AutoCloseable {
    static final int DEFAULT_SIZE = 1024 * 1024;
    
    Node head;
    Node current;
    int blockSize;
    
    private class Node {
        Node(int blockSize) {
            this.buffer = MemoryManager.alloc(blockSize);
            this.address = UberUtil.getAddress(this.buffer);
        }
        
        ByteBuffer buffer;
        Node next;
        long address;
        long startPosition;
    }

    /* keep it for debug purpose
    @Override
    protected void finalize() throws Throwable {
        if (this.head != null) {
            System.err.println("leak!!!!");
            this.callstack.printStackTrace();
        }
    }
    */

    public FlexibleHeap () {
        this(DEFAULT_SIZE);
    }
    
    public FlexibleHeap (int blockSize) {
        this.blockSize = blockSize;
        this.head = new Node(this.blockSize);
        this.head.startPosition = 0;
        this.current = this.head;
    }
    
    /**
     * 
     * @param size
     * @return address/pointer of the allocated address
     */
    @Override
    public final long alloc(int size, boolean zero) {
        if (this.current.buffer.remaining() < (size)) {
            int allocSize = Math.max(size, this.blockSize);
            Node node = allocNode(allocSize);
            node.startPosition = this.current.startPosition + this.current.buffer.capacity();
            this.current.next = node;
            this.current = node;
        }
        ByteBuffer buf = this.current.buffer;
        int pos = buf.position();
        long address = this.current.address + pos;
        buf.position(pos + size);
        if (zero) {
            Unsafe.setMemory(address, size, (byte)0);
        }
        return address;
    }
    
    private Node allocNode(int size) {
        Node result = null;
        
        // first try to reuse existing buffer
        
        if (this.current.next != null) {
            Node next = this.current.next;
            if (next.buffer.capacity() >= size) {
                next.buffer.clear();
                result = next;
            }
        }
        
        if (result == null) {
            result = new Node(size);
            if (this.current.next != null) {
                // chain the reusable blocks
                result.next = this.current.next;
            }
        }
        
        // done
        
        return result;
    }

    public final void reset(long pos) {
        this.current = this.head;
        for (Node i=this.head; i!=null; i=i.next) {
            long posInBuffer = pos - i.startPosition;
            if ((posInBuffer >= 0) && (posInBuffer < i.buffer.capacity())) {
                i.buffer.position((int)posInBuffer);
                return;
            }
        }
        throw new IllegalArgumentException();
    }

    public long write(ByteBuffer buf) {
        int size = buf.remaining();
        long addr = alloc(size);
        while (buf.hasRemaining()) {
            byte bt = buf.get();
            Unsafe.putByte(addr, bt);
            addr++;
        }
        return addr;
    }
    
    public long position() {
        return this.current.buffer.position();
    }

    public ByteBuffer getBytes() {
        ByteBuffer dup = this.current.buffer.duplicate();
        dup.flip();
        return dup;
    }

    @Override
    public void free() {
        for (Node i=this.head; i!=null; i=i.next) {
            MemoryManager.free(i.buffer);
        }
        this.head = null;
    }

    @Override
    public void close() {
        free();
    }
}
