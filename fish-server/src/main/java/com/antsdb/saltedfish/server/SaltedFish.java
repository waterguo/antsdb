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
package com.antsdb.saltedfish.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import com.antsdb.saltedfish.server.mysql.MysqlChannelInitializer;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.util.UberUtil;

public class SaltedFish {
    static Logger _log = UberUtil.getThisLogger();
    static SaltedFish _singleton;
    
    Orca orca;
    ConfigService configService;
    File home;
    ChannelFuture f;
    EventLoopGroup bossGroup;
    NioEventLoopGroup workerGroup;
    boolean isClosed;

    public SaltedFish(File home) {
        _singleton = this;
        this.home = home;
        System.out.println("satled fish home: " + home.getAbsolutePath());
    }

    public void start() throws Exception {
        try {
            this.configService = new ConfigService(new File(getConfigFolder(home), "conf.properties"));
            startLogging();
            // MUST start netty before starting database. use the port to lock out database instance. otherwise, the
            // 2nd database will corrupt the database files.
            startNetty();
            startDatabase();
        }
        catch (Exception x) {
            _log.error("failed to start", x);
            throw x;
        }
    }
    
    public void startOrcaOnly() throws Exception {
        startLogging();
        
        this.configService = new ConfigService(new File(getConfigFolder(home), "conf.properties"));
        
        // disable hbase service by removing hbase conf
        Properties props = this.configService.getProperties();
        props.remove("hbase_conf");
        
        startDatabase();
    }

    void startLogging() {
    	    Pattern ptn = Pattern.compile("log4j\\.appender\\..+\\.file");
        Properties props = getLoggingConf();
        for (Map.Entry<Object, Object> i:props.entrySet()) {
        	String key = (String)i.getKey();
        	if (!ptn.matcher(key).matches()) {
        		continue;
        	}
        	String value = (String)i.getValue();
        	File file = new File(this.home, value);
        	i.setValue(file.getAbsolutePath());
        	    System.out.println("log file: " + file.getAbsolutePath());
        }
        PropertyConfigurator.configure(props);
    }
    
    Properties getLoggingConf() {
        File logConf = new File(getConfigFolder(home), "log4j.properties");
        if (!logConf.exists()) {
            logConf = new File(getConfigFolder(home), "conf/log4j.properties");
        }
        Properties props = new Properties();
        if (logConf.exists()) {
            System.out.println("using log configuration: " + logConf.getAbsolutePath());
            try (FileInputStream in=new FileInputStream(logConf)) {
                props.load(in);
                return props;
            } 
            catch (Exception ignored) {}
        }
        try (InputStream in=getClass().getResourceAsStream("/log4j.properties")) {
            System.out.println("using log configuration: " + getClass().getResource("/log4j.properties"));
            props.load(in);
        	    return props;
        }
        catch (Exception ignored) {}
        return props;
    }
    
    public void run() throws Exception {
        start();
        f.channel().closeFuture();
    }
    
    void startDatabase() throws Exception {        
    	    Orca orca = new Orca(this.home, this.configService.getProperties());
    	    this.orca = orca;
    }
    
    /**
     * starting netty
     * 
     * @param port
     * @throws InterruptedException 
     */
    void startNetty() throws Exception {
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup(this.configService.getNettyWorkerThreadPoolSize());
        _log.info("java.home: {}", System.getProperty("java.home"));
        _log.info("netty worker pool size: {}", workerGroup.executorCount());

        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
         .channel(NioServerSocketChannel.class)
         .childHandler(new MysqlChannelInitializer(this))
         .option(ChannelOption.SO_BACKLOG, 128)
         .childOption(ChannelOption.SO_KEEPALIVE, true);

        // Bind and start to accept incoming connections.

        _log.info("starting netty on port: " + this.configService.getPort());
        this.f = b.bind(this.configService.getPort()).sync();
    }

    public void shutdown() {
        try {
            this.orca.shutdown();
        }
        catch (Exception x) {
            _log.error("unable to shutdown orca gracefully", x);
        }
        
        // Wait until the server socket is closed.
        // In this example, this does not happen, but you can do that to gracefully
        // shut down your server.
        if (workerGroup != null) {
        	workerGroup.shutdownGracefully();
        }
        if (bossGroup != null) {
        	bossGroup.shutdownGracefully();
        }
    }
    
    public Orca getOrca() {
        return this.orca;
    }

    public static SaltedFish getInstance() {
        return _singleton;
    }

	public ConfigService getConfig() {
		return configService;
	}

	public File getConfigFolder(File home) {
		File conf = new File(home, "conf");
		return (conf.exists()) ? conf : home;
	}
	
	public void close() {
	    this.isClosed = true;
	    this.orca.shutdown();
	}
	
	public boolean isClosed() {
	    return this.isClosed;
	}
}
