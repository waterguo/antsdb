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
package com.antsdb.saltedfish.backup;

import java.io.DataOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import org.apache.commons.cli.Options;

import com.antsdb.mysql.network.MysqlClient;
import com.antsdb.saltedfish.util.MysqlJdbcUtil;

/**
 * 
 * @author *-xguo0<@
 */
public class FishBackupMain extends JdbcBackupMain {

    private String host;
    private String port;
    
    public static void main(String[] args) throws Exception {
        MysqlJdbcUtil.loadClass();
        new FishBackupMain().parseAndRun(args);
    }

    @Override
    protected void buildOptions(Options options) {
        options.addOption(null, "host", true, "host");
        options.addOption(null, "port", true, "port");
    }

    @Override
    protected void run() throws Exception {
        this.host = this.cmdline.getOptionValue("host");
        this.port = this.cmdline.getOptionValue("post");
        
        if (this.host == null) {
            this.host = "localhost";
        }
        if (this.port == null) {
            this.port = "3306";
        }

        BackupFile backup = getBackupInfo();
        
        backup.save(System.out);
        
        saveDump(System.out, backup);
    }

    private void saveDump(OutputStream out, BackupFile backup) throws Exception {
        DataOutputStream dout = new DataOutputStream(out);
        WritableByteChannel ch = Channels.newChannel(dout);
        MysqlClient client = new MysqlClient(this.host, Integer.parseInt(this.port));
        client.connect();
        client.login("test", "test");
        for (TableBackupInfo table:backup.tables) {
            client.backup(table.getFullName());
            dout.writeUTF(table.getFullName());
            for (;;) {
                ByteBuffer packet = client.readPacketErrorCheck();
                if (client.isEof(packet)) {
                    break;
                }
                packet.position(4);
                ch.write(packet);
            }
            dout.writeInt(0);
        }
        dout.flush();
    }
}
