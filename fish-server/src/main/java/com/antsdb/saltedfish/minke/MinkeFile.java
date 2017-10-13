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
package com.antsdb.saltedfish.minke;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.nosql.MemoryMappedFile;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * a physical file that consists of cache 
 * 
 * @author *-xguo0<@
 */
class MinkeFile implements Closeable {
	static final int HEADER_SIZE = 1024;
	static final int SIGNATURE = 0x73746e61;
	static final int OFFSET_SIG = 0;
    static final int OFFSET_LOG_FILE_SIZE = 0x10;
    static final int OFFSET_LOG_PAGE_SIZE = 0x14;
	
	static final Logger _log = UberUtil.getThisLogger();
	
	File file;
	private MemoryMappedFile mmf;
    private int pageSize;
    private int fileSize;
    private int nPages;
    long addr;
    private MinkePage[] pages;
    private int fileId;
    private boolean isMutable;
	
    public MinkeFile(int fileId, File file, int fileSize, int pageSize, boolean isMutable) {
        this.file = file;
        this.fileId = fileId;
        this.pageSize = pageSize;
        this.fileSize = fileSize;
        this.nPages = (fileSize - HEADER_SIZE) / this.pageSize;
        this.pages = new MinkePage[this.nPages];
        this.isMutable = isMutable;
    }


    public void open() throws IOException {
        if (this.file.exists()) {
            this.mmf = new MemoryMappedFile(file, this.isMutable ? "rw" : "r");
        }
        else {
            this.mmf = new MemoryMappedFile(file, this.fileSize, this.isMutable ? "rw" : "r");
        }
        this.addr = this.mmf.getAddress();
        if (getSignature() == SIGNATURE) {
            this.fileSize = getFileSize();
            this.pageSize = getPageSize();
        }
        else if (this.isMutable){
            setSignature(SIGNATURE);
            setFileSize(this.fileSize);
            setPageSize(this.pageSize);
        }
        for (int i=0; i<this.pages.length; i++) {
            int pageId = (this.fileId << 16) + i;
            this.pages[i] = getPage(pageId);
        }
    }

    @Override
    public void close() throws IOException {
        if (this.mmf == null) {
            return;
        }
        this.mmf.close();
        this.mmf = null;
    }

    @Override
    public String toString() {
        return this.mmf.toString();
    }

    public MinkePage getPage(int pageId) {
        int idx = pageId & 0xffff;
        MinkePage result = this.pages[idx];
        if (result != null) {
            return result;
        }
        synchronized (this) {
            result = this.pages[idx];
            if (result != null) {
                return result;
            }
            long pPage = getPageAddress(idx);
            result = new MinkePage(this, pPage, this.pageSize, pageId);
            this.pages[idx] = result;
            return result;
        }
    }
    
    private long getPageAddress(long idx) {
        long result = this.mmf.getAddress() + HEADER_SIZE + idx * this.pageSize;
        return result;
    }
    
    MinkePage[] getPages() {
        return this.pages;
    }

    void force(MinkePage page) throws IOException {
        long offset = page.addr - this.addr;
        this.mmf.force(offset, this.pageSize);
    }
    
    public int getPageSize() {
        return Unsafe.getInt(this.addr + OFFSET_LOG_PAGE_SIZE);
    }
    
    public void setPageSize(int value) {
        Unsafe.putInt(this.addr + OFFSET_LOG_PAGE_SIZE, value);
    }
    
    public int getFileSize() {
        return Unsafe.getInt(this.addr + OFFSET_LOG_FILE_SIZE);
    }
    
    public void setFileSize(int value) {
        Unsafe.putInt(this.addr + OFFSET_LOG_FILE_SIZE, value);
    }
    
    public int getSignature() {
        return Unsafe.getInt(this.addr + OFFSET_SIG);
    }
    
    public void setSignature(int value) {
        Unsafe.putInt(this.addr + OFFSET_SIG, value);
    }
}
