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
package com.antsdb.saltedfish.parquet;

import com.antsdb.saltedfish.util.BytesUtil;
/**
 * 
 * @author Frank Li<lizc@tg-hd.com>
 */
public class Scan {
    private byte[] startRow;
    private byte[] stopRow;
    private boolean reversed;
    private boolean incStart;
    private boolean incEnd;
    

    public byte[] getStartRow() {
        return startRow;
    }

    public void setStartRow(byte[] startRow) {
        this.startRow = startRow;
    }

    public byte[] getStopRow() {
        return stopRow;
    }

    public void setStopRow(byte[] stopRow) {
        this.stopRow = stopRow;
    }

    public boolean isReversed() {
        return reversed;
    }

    public String toString() {
        return String.format("startRow=%s stopRow=%s reversed=%s incStart=$s incEnd=%s",  
                BytesUtil.toHex(startRow),
                BytesUtil.toHex(stopRow),
                reversed,
                incStart,
                incEnd
                );
    }

    public void setReversed(boolean reversed) {
        this.reversed = reversed;
    }

    public boolean isIncStart() {
        return incStart;
    }

    public void setIncStart(boolean incStart) {
        this.incStart = incStart;
    }

    public boolean isIncEnd() {
        return incEnd;
    }

    public void setIncEnd(boolean incEnd) {
        this.incEnd = incEnd;
    }
    
}
