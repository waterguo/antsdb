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
package com.antsdb.saltedfish.sql.vdm

import static org.junit.Assert.*;

import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test

import com.antsdb.saltedfish.TestHBaseBase
import com.antsdb.saltedfish.minke.MinkePage;
import com.antsdb.saltedfish.minke.MinkeTable;
import com.antsdb.saltedfish.nosql.GTable;

import groovy.sql.Sql;;

/**
 * 
 * @author *-xguo0<@
 */
class TestEvictCachePage extends TestHBaseBase {
    Sql db;
    
    @Before
    public void before() throws SQLException {
        this.db = new Sql(getConnection());
    }
    
    @After
    public void after() {
        db.close();
        db = null;
    }

    @Test
    public void test() {
        db.execute("DROP TABLE IF EXISTS test");
        db.execute("CREATE TABLE test (id int PRIMARY KEY, name varchar(100))");
        for (int i=0; i<200; i++) {
            String sql = String.format("INSERT INTO test VALUES(%d,'%d')", i, i);
            db.execute(sql);
        }
        GTable gtable = humpback.findTable("TEST", "test");
        MinkeTable mtable = (MinkeTable)getCache().getMinke().getTable(gtable.id);
        db.execute(".REORGANIZE");
        int pageCount = mtable.getPageCount();
        assertTrue(pageCount > 1);
        MinkePage firstPage = mtable.getPages().getAt(0);
        db.execute(".EVICT CACHE PAGE " + firstPage.getId());
        assertEquals(pageCount-1, mtable.getPageCount());
    }
}
