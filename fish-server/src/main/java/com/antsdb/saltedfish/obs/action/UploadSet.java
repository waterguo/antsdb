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
package com.antsdb.saltedfish.obs.action;

import java.util.ArrayList;
import java.util.List;

public class UploadSet {
    
    public final static String UPLOADSET_FILENAME = "upload-set.json";

    List<ActionDeleteFolder> dropActions;
    
    List<ActionUploadFile> uploadActions;
    
    List<ActionDeleteFile> deleteActions;
    
    private ActionUploadSyncParam uploadSyncParam ;

    public UploadSet() {
        this.dropActions = new ArrayList<>();
        this.uploadActions = new ArrayList<>();
        this.deleteActions = new ArrayList<>();
    }

    public ActionUploadSyncParam getUploadSyncParam() {
        return uploadSyncParam;
    }

    public void setUploadSyncParam(ActionUploadSyncParam uploadSyncParam) {
        this.uploadSyncParam = uploadSyncParam;
    }

    public List<ActionDeleteFolder> getDropActions() {
        return dropActions;
    }

    public void setDropActions(List<ActionDeleteFolder> dropActions) {
        this.dropActions = dropActions;
    }

    public List<ActionUploadFile> getUploadActions() {
        return uploadActions;
    }

    public void setUploadActions(List<ActionUploadFile> uploadActions) {
        this.uploadActions = uploadActions;
    }

    public List<ActionDeleteFile> getDeleteActions() {
        return deleteActions;
    }

    public void setDeleteActions(List<ActionDeleteFile> deleteActions) {
        this.deleteActions = deleteActions;
    }  
}
