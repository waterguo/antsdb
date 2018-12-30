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

import com.antsdb.saltedfish.lexer.FishParser.AssignmentContext;

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
}
