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
package com.antsdb.saltedfish.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;

import com.antsdb.saltedfish.nosql.GTableReadOnly;
import com.antsdb.saltedfish.nosql.HumpbackReadOnly;
import com.antsdb.saltedfish.nosql.Row;
import com.antsdb.saltedfish.nosql.RowIterator;
import com.antsdb.saltedfish.sql.meta.TableId;
import com.antsdb.saltedfish.sql.vdm.KeyMaker;
import com.antsdb.saltedfish.sql.vdm.SysColumnRow;
import com.antsdb.saltedfish.sql.vdm.SysRuleColumnRow;
import com.antsdb.saltedfish.sql.vdm.SysRuleRow;
import com.antsdb.saltedfish.sql.vdm.SysTableRow;

/**
 * 
 * @author wgu0
 */
public class FishMetaUtilMain extends FishCommandLine {

	private HumpbackReadOnly humpback;

	public static void main(String[] args) throws Exception {
		new FishMetaUtilMain(args).run();
	}

	protected Options getOptions() {
		Options options = new Options();
		options.addOption("h", "help", false, "print help");
		options.addOption("t", "table", true, "find table by name or id, case insensitive");
		options.addOption(null, "home", true, "antsdb home");
		options.addOption(null, "tables", false, "list all tables");
		return options;
	}
	
	public FishMetaUtilMain(String[] args) throws ParseException {
		super(args);
	}
	
	private void run() throws Exception {
		CommandLine line = this.cmd;
		
		if (line.getOptions().length == 0 || line.hasOption('h')) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("antsdb-meta", getOptions());
			return;
		}
		
		this.humpback = getHumpbackReadOnly();
		if (this.humpback == null) {
		    return;
		}
		if (line.hasOption('t')) {
			findTable(line.getOptionValue('t'));
			return;
		}
		if (line.hasOption("tables")) {
			listTables();
			return;
		}
	}

	private void listTables() {
		GTableReadOnly gtable = humpback.getTable(-TableId.SYSTABLE.ordinal());
		if (gtable == null) {
			println("error: SYSTABLE is not found");
			return;
		}
		for (RowIterator i=gtable.scan(0, Long.MAX_VALUE);;) {
			if (!i.next()) {
				break;
			}
			SysTableRow row = new SysTableRow(i.getRow());
			println(row.toString());
		}
	}

	private void findTable(String name) throws Exception {
		GTableReadOnly gtable = humpback.getTable(-TableId.SYSTABLE.ordinal());
		if (gtable == null) {
			println("error: SYSTABLE is not found");
			return;
		}
		for (RowIterator i=gtable.scan(0, Long.MAX_VALUE);;) {
			if (!i.next()) {
				break;
			}
			SysTableRow row = new SysTableRow(i.getRow());
			if (String.valueOf(row.getId()).equals(name)) {
				dump(row);
				return;
			}
			if ((row.getNamespace() + "." + row.getTableName()).equalsIgnoreCase(name)) {
				dump(row);
                return;
			}
		}
		println("error: table not found");
	}

	private void dump(SysTableRow table) {
		println(table.toString());
		GTableReadOnly syscolumns = this.humpback.getTable(-TableId.SYSCOLUMN.ordinal());
		println("    columns");
		for (RowIterator i=syscolumns.scan(0, Long.MAX_VALUE);;) {
			if (!i.next()) {
				break;
			}
			SysColumnRow row = new SysColumnRow(i.getRow());
			if (!row.getNamespace().equals(table.getNamespace())) {
				continue;
			}
			if (!row.getTableName().equals(table.getTableName())) {
				continue;
			}
			println("        %s", row.toString());
		}
		println("    keys");
		Map<Integer, List<SysRuleColumnRow>> ruleColumnsById = new HashMap<>();
		GTableReadOnly sysrulecols = this.humpback.getTable(-TableId.SYSRULECOL.ordinal());
		for (RowIterator i=sysrulecols.scan(0, Long.MAX_VALUE);;) {
			if (!i.next()) {
				break;
			}
			SysRuleColumnRow row = new SysRuleColumnRow(i.getRow());
			List<SysRuleColumnRow> list = ruleColumnsById.get(row.getRuleId());
			if (list == null) {
				list = new ArrayList<>();
				ruleColumnsById.put(row.getRuleId(), list);
			}
			list.add(row);
		}
		GTableReadOnly sysrules = this.humpback.getTable(-TableId.SYSRULE.ordinal());
		for (RowIterator i=sysrules.scan(0, Long.MAX_VALUE);;) {
			if (!i.next()) {
				break;
			}
			SysRuleRow row = new SysRuleRow(i.getRow());
			if (row.getTableId() != table.getId()) {
				continue;
			}
			List<SysRuleColumnRow> ruleColumns = ruleColumnsById.get(row.getId());
			List<String> columns = new ArrayList<>();
			if (ruleColumns != null) {
				for (SysRuleColumnRow j:ruleColumns) {
					Row jj = syscolumns.getRow(0, Long.MAX_VALUE, KeyMaker.make(j.getRuleColumnId()));
					if (jj != null) {
						SysColumnRow jjj = new SysColumnRow(jj);
						columns.add(jjj.getColumnName());
					}
				}
			}
			println("        %s (%s)", row.toString(), StringUtils.join(columns, ',')); 
		}
	}
}
