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
package com.antsdb.saltedfish.sql.vdm;

import java.util.List;

import org.slf4j.Logger;

import com.antsdb.saltedfish.cpp.Heap;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.HumpbackError;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.VaporizingRow;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.PrimaryKeyMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * common logic for UPDATE statement
 *  
 * @author wgu0
 */
abstract class UpdateBase extends Statement {
	static Logger _log = UberUtil.getThisLogger();
	
    GTable gtable;
    List<ColumnMeta> columns;
    List<Operator> values;
	IndexEntryHandlers indexHandlers;
	TableMeta table;
	boolean isPrimaryKeyAffected = false;

    public UpdateBase(Orca orca, TableMeta table, GTable gtable, List<ColumnMeta> columns, List<Operator> values) {
		super();
		this.gtable = gtable;
		this.columns = columns;
		this.values = values;
		this.table = table;
        this.indexHandlers = new IndexEntryHandlers(orca, table);
        PrimaryKeyMeta pk = table.getPrimaryKey();
        if (pk != null) {
	        List<ColumnMeta> pkColumns = table.getPrimaryKey().getColumns(table);
	        for (ColumnMeta i:columns) {
	            if (pkColumns.contains(i)) {
    	        		    this.isPrimaryKeyAffected = true;
    	        		    break;
    	        	    }
	        }
        }
	}

	protected boolean updateSingleRow(VdmContext ctx, Heap heap, Parameters params, long pKey) {
        	boolean success = false;
        	Transaction trx = ctx.getTransaction();
        	trx.getGuaranteedTrxId();
        	int timeout = ctx.getSession().getConfig().getLockTimeout();
        	long heapMark = heap.position();
        for (;;) {
        	heap.reset(heapMark);
            // get the __latest__ version of the row 

            Row oldRow = this.gtable.getRow(trx.getTrxId(), Long.MAX_VALUE, pKey);
	        if (oldRow == null) {
	            // row could be deleted between query and here
	            return false;
	        }
	        long pRecord = Record.fromRow(heap, table, oldRow);
	        VaporizingRow newRow = VaporizingRow.from(heap, this.table.getMaxColumnId(), oldRow);
	        
            // update new values
            
            for (int i=0; i<this.columns.size(); i++) {
                ColumnMeta column = this.columns.get(i);
                Operator expr = this.values.get(i);
                long pValue = expr.eval(ctx, heap, params, pRecord);
                newRow.setFieldAddress(column.getColumnId(), pValue);
            }
            
            // update storage

            HumpbackError error;
            boolean primaryKeyChange = false;
            if (this.isPrimaryKeyAffected) {
            	long pNewKey = this.table.getKeyMaker().make(heap, newRow);
            	primaryKeyChange = !KeyMaker.equals(pKey, pNewKey);
            	newRow.setKey(pNewKey);
            }
            if (primaryKeyChange) {
            	error = this.gtable.delete(trx.getTrxId(), pKey, timeout);
            	if (error != HumpbackError.SUCCESS) {
                	throw new OrcaException(error);
            	}
            	newRow.setVersion(trx.getTrxId());
            	error = this.gtable.insert(newRow, timeout);
            }
            else {
            	newRow.setVersion(trx.getTrxId());
                error = this.gtable.update(newRow, oldRow.getTrxTimestamp(), timeout);
            }
            if (error == HumpbackError.SUCCESS) {
            	// update indexes
            	this.indexHandlers.update(heap, trx, oldRow, newRow, primaryKeyChange, timeout);
            	success = true;
            	break;
            }
            else if (error == HumpbackError.CONCURRENT_UPDATE) {
            	// row is updated by another trx. retry
            	continue;
            }
            else {
            	throw new OrcaException(error);
            }
        }
        return success;
	}
}
