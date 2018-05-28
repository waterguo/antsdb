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

/**
 * this is a Murmur Hash 2.0 implementation using direction memory. 
 * https://github.com/tnm/murmurhash-java/blob/master/src/main/java/ie/ucd/murmur/MurmurHash.java is used as a 
 * reference
 *  
 * @author *-xguo0<@
 */
public final class MurmurHash {
    final static int DEFAULT_SEED = 0x20070211; 
            
    public static long hash64(long p, int size, int seed) {
        final long m = 0xc6a4a7935bd1e995L;
        final int r = 47;
        long h = (seed&0xffffffffl)^(size*m);
        int size8 = size/8;
        for (int i=0; i<size8; i++) {
            final int i8 = i*8;
            long k = Unsafe.getLong(p + i8);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h ^= k;
            h *= m; 
        }    
        switch (size % 8) {
            case 7: h ^= (long)(Unsafe.getByte(p + (size & ~7) + 6) & 0xff) << 48;
            case 6: h ^= (long)(Unsafe.getByte(p + (size & ~7) + 5) & 0xff) << 40;
            case 5: h ^= (long)(Unsafe.getByte(p + (size & ~7) + 4) & 0xff) << 32;
            case 4: h ^= (long)(Unsafe.getByte(p + (size & ~7) + 3) & 0xff) << 24;
            case 3: h ^= (long)(Unsafe.getByte(p + (size & ~7) + 2) & 0xff) << 16;
            case 2: h ^= (long)(Unsafe.getByte(p + (size & ~7) + 1) & 0xff) << 8;
            case 1: h ^= (long)(Unsafe.getByte(p + (size & ~7) + 0) & 0xff);
                    h *= m;
        };
        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;
        return h;    
    }
    
    public static long hash64(long p, int size) {
        return hash64(p, size, DEFAULT_SEED);
    }}
