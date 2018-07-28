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
package com.antsdb.saltedfish.minke;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.Unsafe;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * warms up the cache 
 *  
 * @author *-xguo0<@
 */
public class Warmer implements Callable<Integer> {
    final static Logger _log = UberUtil.getThisLogger();
    
    private Minke minke;
    boolean first = true;
    ByteBuffer buf;
    private long target;
    
    public Warmer(Minke minke, long size) {
        this.minke = minke;
        this.target = size;
    }
    
    @Override
    public Integer call() throws Exception {
        warm();
        return null;
    }
    
    private List<MinkeFile> getFiles() {
        List<MinkeFile> files = new ArrayList<>();
        for (MinkeFile i:this.minke.getFiles()) {
            if (i != null) {
                files.add(i);
            }
        }
        return files;
    }
    
    @SuppressWarnings("unused")
    private void warmFromCold() {
        _log.debug("starting warmer ...");
        List<MinkeFile> files = getFiles();
        Collections.sort(files, new Comparator<MinkeFile>() {
            @Override
            public int compare(MinkeFile x, MinkeFile y) {
                return Long.compare(y.file.lastModified(), x.file.lastModified());
            }
        });
        long warmed = 0;
        int checksum = 0;
        for (MinkeFile mfile:files) {
            try (FileChannel ch=openFile(mfile); FileChannel nul=openNull()) {
                for (MinkePage mpage:mfile.getPages()) {
                    int state = mpage.getState();
                    if ((state == PageState.ACTIVE) || (state == PageState.CARBONFREEZED)) {
                        checksum = checksum ^ warm(ch, nul, mpage);
                        warmed += mpage.getUsage();
                    }
                    if (warmed >= this.target) {
                        break;
                    }
                }
                if (warmed >= this.target) {
                    break;
                }
            }
            catch (IOException x) {
                _log.warn("", x);
            }
        }
        _log.debug("{} bytes have been warmed {}", warmed, checksum);
    }

    private FileChannel openFile(MinkeFile mfile) throws IOException {
        Path path = mfile.file.toPath();
        FileChannel ch = FileChannel.open(path, StandardOpenOption.READ);
        return ch;
    }

    private void warm() throws IOException {
        // find the first x pages ordered by last access time
        
        _log.debug("starting warmer ...");
        int size = (int)(this.target / this.minke.getPageSize());
        PriorityQueue<MinkePage> candidates = new PriorityQueue<>(size + 1, new Comparator<MinkePage>() {
            @Override
            public int compare(MinkePage x, MinkePage y) {
                return Long.compare(y.lastAccess.get(), x.lastAccess.get());
            }
        });
        for (MinkeFile mfile:this.minke.getFiles()) {
            if (mfile == null) {
                continue;
            }
            for (MinkePage mpage:mfile.getPages()) {
                int state = mpage.getState();
                if ((state == PageState.ACTIVE) || (state == PageState.CARBONFREEZED)) {
                    candidates.add(mpage);
                }
                while (candidates.size() > size) {
                    candidates.poll();
                }
            }
        }
        
        // reorder the pages by id , taking advantage of the sequential read
        
        long warmed = 0;
        ArrayList<MinkePage> pages = new ArrayList<>(candidates);
        pages.sort(new Comparator<MinkePage>() {
            @Override
            public int compare(MinkePage x, MinkePage y) {
                return Integer.compare(x.id, y.id);
            }
        });
        FileChannel nul = openNull();
        for (MinkePage mpage:pages) {
            warm(mpage, nul);
            warmed += mpage.getUsage();
        }
        _log.debug("{} bytes have been warmed", warmed);
    }

    private void warm(MinkePage mpage, FileChannel nul) throws IOException {
        FileChannel ch = mpage.mfile.mmf.getChannel();
        int usage = mpage.getUsage();
        long offset = mpage.addr - mpage.mfile.addr;
        ch.transferTo(offset, usage, nul);
    }

    private int warm(FileChannel ch, FileChannel nul, MinkePage mpage) throws IOException {
        int usage = mpage.getUsage();
        long offset = mpage.addr - mpage.mfile.addr;
        ch.transferTo(offset, usage, nul);
        int result = 0;
        for (int i=0; i<usage; i+=1024) {
            result = result ^ Unsafe.getByte(mpage.addr + i);
        }
        return result;
    }
    
    private FileChannel openNull() throws IOException {
        return FileChannel.open(new File("/dev/null").toPath(), StandardOpenOption.WRITE);
    }
}
