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

import java.io.IOException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

public class KerberosCallbackHandler implements CallbackHandler {
	/**
	 * Password callback handler for resolving password/usernames for a JAAS login.
	 * @author Ants
	 */
	  public KerberosCallbackHandler() {
	    super();
	  }
	 
	  public KerberosCallbackHandler( String name, String password) {
	    super();
	    this.username = name;
	    this.password = password;
	  }
	 
	  public KerberosCallbackHandler( String password) {
	    super();
	    this.password = password;
	  }
	 
	  private String password;
	  private String username;
	 
	  /**
	   * Handles the callbacks, and sets the user/password detail.
	   * @param callbacks the callbacks to handle
	   * @throws IOException if an input or output error occurs.
	   */
	  public void handle( Callback[] callbacks)
	      throws IOException, UnsupportedCallbackException {
	 
	    for ( int i=0; i<callbacks.length; i++) {
	      if ( callbacks[i] instanceof NameCallback && username != null) {
	        NameCallback nc = (NameCallback) callbacks[i];
	        nc.setName( username);
	      }
	      else if ( callbacks[i] instanceof PasswordCallback) {
	        PasswordCallback pc = (PasswordCallback) callbacks[i];
	        pc.setPassword( password.toCharArray());
	      }
	      else {
	        /*throw new UnsupportedCallbackException(
	        callbacks[i], "Unrecognized Callback");*/
	      }
	    }
	  }
}
