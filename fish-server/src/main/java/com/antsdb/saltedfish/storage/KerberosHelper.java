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
package com.antsdb.saltedfish.storage;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.slf4j.Logger;

import com.antsdb.saltedfish.util.UberUtil;

public class KerberosHelper {
	static String _realm;
	static String _kdc;
	static String _jaasConf;
    static Logger _log = UberUtil.getThisLogger();
	
	public static void initialize(String realm, String kdc, String jaasConf) {
		_realm = realm;
		_kdc = kdc;
		_jaasConf = jaasConf;
	}
	
	public static boolean login(String userid, String password) {
		// Kerberos is not configured properly, return false
		if (_realm == null || _realm.isEmpty() || 
				_kdc == null || _kdc.isEmpty() || _jaasConf == null) {
			_log.error("Kerberos auth is not configured.");
			return false;
		}
		
		boolean result = false;

		// 1. Set up Kerberos properties - have to set through java -D....
		//  -Djava.security.krb5.realm=BLUE-ANTS.CLOUDAPP.NET 
		//  -Djava.security.krb5.kdc=blue-ants.cloudapp.net
		//  -Djava.security.auth.login.config=/Users/kylechen/antsdb/0705/salted-fish/fish-server/hbase-db/jaas.conf
        System.setProperty("java.security.auth.login.config", _jaasConf);
		System.setProperty("java.security.krb5.realm", _realm);  
		System.setProperty("java.security.krb5.kdc", _kdc);  

        System.setProperty("sun.security.krb5.debug", "true");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "true");

        // 2. Authenticate against the KDC using JAAS and return the Subject.
        LoginContext loginCtx = null;
        // "Client" references the corresponding JAAS configuration section in the jaas.conf file.
        KerberosCallbackHandler callback = new KerberosCallbackHandler(userid, password);
        try {
          loginCtx = new LoginContext("Client", callback);
          loginCtx.login();
        }
        catch ( LoginException e) {
        	_log.error("Client: There was an error during the JAAS login: " + e);
          e.printStackTrace();
          return false;
       }

        Subject subject = loginCtx.getSubject();

        if (subject != null) result = true;

        return result;
	}

}
