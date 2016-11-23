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
package com.antsdb.saltedfish.storage;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import com.antsdb.saltedfish.cpp.FlexibleHeap;
import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.sql.meta.TableMeta;

/**
 * 
 * @author wgu0
 */
class HBaseScanResult implements RowIterator {
	private Heap heap = new FlexibleHeap();
	ResultScanner scanner;
	long pRow;
	long rowScanned = 0;
	TableMeta table;
	
	public HBaseScanResult(ResultScanner result) {
		this.scanner = result;
		this.heap = new FlexibleHeap();
		pRow = this.heap.alloc(1);
	}

	@Override
	public boolean next() {
		try {
			Result r = this.scanner.next();
			if (r == null) {
				this.pRow = 0;
				return false;
			}
			this.rowScanned++;
			this.heap.reset(0);
			/* 
			 * Helper.toRow(r, table);
			 */
			return true;
		}
		catch (Exception x) {
			throw new OrcaHBaseException(x);
		}
	}

	@Override
	public long getRowKeyPointer() {
		return 0;
	}

	@Override
	public long getRowPointer() {
		return this.pRow;
	}

	@Override
	public long getVersion() {
		return Row.getVersion(this.pRow);
	}

	@Override
	public long getRowScanned() {
		return this.rowScanned;
	}

	@Override
	public void rewind() {
		throw new NotImplementedException();
	}

	@Override
	public Row getRow() {
		return Row.fromMemoryPointer(getRowKeyPointer(), getVersion());
	}

	@Override
	public void close() {
	}

	@Override
	public long getKeyPointer() {
		return 0;
	}

	@Override
	public boolean isRow() {
		throw new NotImplementedException();
	}

	@Override
	public long getIndexSuffix() {
		throw new NotImplementedException();
	}

	@Override
	public boolean eof() {
		throw new NotImplementedException();
	}

	@Override
	public byte getMisc() {
		throw new NotImplementedException();
	}
}
