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

import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.schema.MessageType;

import com.antsdb.saltedfish.nosql.StorageTable;

public class ReplicationAssistant {
    private int tableId;
    private Mapping mapping = null;
    private MessageType schema = null;
    private GroupFactory factory = null;
    private TableName tableInfo;
    private StorageTable table;

    public StorageTable getTable() {
        return table;
    }

    public void setTable(StorageTable table) {
        this.table = table;
    }

    public int getTableId() {
        return tableId;
    }

    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    public Mapping getMapping() {
        return mapping;
    }

    public void setMapping(Mapping mapping) {
        this.mapping = mapping;
    }

    public MessageType getSchema() {
        return schema;
    }

    public void setSchema(MessageType schema) {
        this.schema = schema;
    }

    public GroupFactory getFactory() {
        return factory;
    }

    public void setFactory(GroupFactory factory) {
        this.factory = factory;
    }

    public TableName getTableInfo() {
        return tableInfo;
    }

    public void setTableInfo(TableName tableInfo) {
        this.tableInfo = tableInfo;
    }
}
