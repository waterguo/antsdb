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

import java.util.List;
import java.util.Properties;

import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.lexer.FishParser.AssignmentContext;
import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.Humpback;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.Session;

/**
 * 
 * @author *-xguo0<@
 */
final class Utils {
    static String getLiteralValue(TerminalNode node) {
        String text = node.getText();
        return text.substring(1, text.length()-1);
    }
    
    static Properties getProperties(List<AssignmentContext> rules) {
        Properties props = new Properties();
        for (AssignmentContext rule:rules) {
            String key = rule.IDENTIFIER().getText().toLowerCase();
            String value = Utils.getLiteralValue(rule.STRING_LITERAL());
            props.put(key.toLowerCase(), value);
        }
        return props;
    }

    static int tableExists(Session session, String name) {
        name = removeQuotes(name);
        Humpback humpback = session.getOrca().getHumpback();
        
        // if the table name is a number
        GTable gtable = null;
        if (StringUtils.isNumeric(name)) {
            gtable = humpback.getTable(Integer.parseInt(name)); 
        }
        
        // find the table by literal name
        if (gtable == null) {
            String ns = session.getCurrentNamespace();
            gtable = humpback.findTable(ns, name);
        }
        
        // error out or done
        if (gtable == null) {
            throw new OrcaException("table {} is not found", name); 
        }
        return gtable.getId();
    }

    private static String removeQuotes(String text) {
        if (text.startsWith("`") || text.startsWith("\"")) {
            text = text.substring(1, text.length() - 1);
        }
        return text;
    }
}
