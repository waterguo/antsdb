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
package com.antsdb.saltedfish.parquet.bean;

public class SyncParam {

    private long currentSp;
    private long serverId;
    private long createTimestamp;
    private long updateTimestamp;
    private String createOrcaVersion;
    private String updateorcaVersion;
    private boolean isActive;

    public long getCurrentSp() {
        return currentSp;
    }

    public void setCurrentSp(long currentSp) {
        this.currentSp = currentSp;
    }

    public long getServerId() {
        return serverId;
    }

    public void setServerId(long serverId) {
        this.serverId = serverId;
    }

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public void setCreateTimestamp(long createTimestamp) {
        this.createTimestamp = createTimestamp;
    }

    public long getUpdateTimestamp() {
        return updateTimestamp;
    }

    public void setUpdateTimestamp(long updateTimestamp) {
        this.updateTimestamp = updateTimestamp;
    }

    public String getCreateOrcaVersion() {
        return createOrcaVersion;
    }

    public void setCreateOrcaVersion(String createOrcaVersion) {
        this.createOrcaVersion = createOrcaVersion;
    }

    public String getUpdateorcaVersion() {
        return updateorcaVersion;
    }

    public void setUpdateorcaVersion(String updateorcaVersion) {
        this.updateorcaVersion = updateorcaVersion;
    }

    public boolean isActive() {
        return isActive;
    }

    public void setActive(boolean isActive) {
        this.isActive = isActive;
    }
    
    public SyncParam clone() throws CloneNotSupportedException {
        SyncParam obj = new SyncParam();
        obj.setCurrentSp(this.getCurrentSp());
        obj.setServerId(this.getServerId());
        obj.setCreateTimestamp(this.createTimestamp);
        obj.setUpdateTimestamp(this.updateTimestamp);
        obj.setCreateOrcaVersion(this.createOrcaVersion);
        obj.setUpdateorcaVersion(this.updateorcaVersion);
        obj.setActive( this.isActive);
        return obj;
    }
}
