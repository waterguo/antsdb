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
package com.antsdb.saltedfish.sql.command;

import java.util.Properties;

import com.antsdb.saltedfish.lexer.FishParser.Backup_stmtContext;
import com.antsdb.saltedfish.obs.StartObsBackup;
import com.antsdb.saltedfish.sql.Generator;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.vdm.Instruction;

/**
 * 
 * @author *-xguo0<@
 */
public class Backup_stmtGenerator extends Generator<Backup_stmtContext> {

    @Override
    public Instruction gen(GeneratorContext ctx, Backup_stmtContext rule) throws OrcaException {
        Properties options = Utils.getProperties(rule.assignment());
        String dest = options.getProperty("antsdb_backup_dest");
        StartObsBackup backup = new StartObsBackup(dest);
        return backup;
    }

}
