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
package com.antsdb.saltedfish.sql.planner;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;

import com.antsdb.saltedfish.nosql.GTable;
import com.antsdb.saltedfish.nosql.SlowRow;
import com.antsdb.saltedfish.sql.DataType;
import com.antsdb.saltedfish.sql.GeneratorContext;
import com.antsdb.saltedfish.sql.Orca;
import com.antsdb.saltedfish.sql.OrcaException;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.PrimaryKeyMeta;
import com.antsdb.saltedfish.sql.meta.RuleColumnMeta;
import com.antsdb.saltedfish.sql.meta.RuleMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.Aggregator;
import com.antsdb.saltedfish.sql.vdm.BinaryOperator;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.DumbDistinctFilter;
import com.antsdb.saltedfish.sql.vdm.DumbGrouper;
import com.antsdb.saltedfish.sql.vdm.DumbSorter;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;
import com.antsdb.saltedfish.sql.vdm.FieldValue;
import com.antsdb.saltedfish.sql.vdm.Filter;
import com.antsdb.saltedfish.sql.vdm.FullTextIndexMergeScan;
import com.antsdb.saltedfish.sql.vdm.GroupByPostProcesser;
import com.antsdb.saltedfish.sql.vdm.IndexRangeScan;
import com.antsdb.saltedfish.sql.vdm.MasterRecordCursorMaker;
import com.antsdb.saltedfish.sql.vdm.NestedJoin;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.OpAnd;
import com.antsdb.saltedfish.sql.vdm.OpEqual;
import com.antsdb.saltedfish.sql.vdm.OpEqualNull;
import com.antsdb.saltedfish.sql.vdm.OpInSelect;
import com.antsdb.saltedfish.sql.vdm.OpLarger;
import com.antsdb.saltedfish.sql.vdm.OpLargerEqual;
import com.antsdb.saltedfish.sql.vdm.OpLess;
import com.antsdb.saltedfish.sql.vdm.OpLessEqual;
import com.antsdb.saltedfish.sql.vdm.OpLike;
import com.antsdb.saltedfish.sql.vdm.OpMatch;
import com.antsdb.saltedfish.sql.vdm.OpOr;
import com.antsdb.saltedfish.sql.vdm.Operator;
import com.antsdb.saltedfish.sql.vdm.RangeScannable;
import com.antsdb.saltedfish.sql.vdm.RecordLocker;
import com.antsdb.saltedfish.sql.vdm.TableRangeScan;
import com.antsdb.saltedfish.sql.vdm.TableScan;
import com.antsdb.saltedfish.sql.vdm.ThroughGrouper;
import com.antsdb.saltedfish.sql.vdm.Union;
import com.antsdb.saltedfish.sql.vdm.Vector;
import com.antsdb.saltedfish.util.CodingError;
import com.antsdb.saltedfish.util.UberUtil;

/**
 * query planner
 * 
 * @author wgu0
 *
 */
public class Planner {
    static Logger _log = UberUtil.getThisLogger();

    private static ColumnMeta KEY = new ColumnMeta(null, new SlowRow(0));
    private static ColumnMeta ROWID = new ColumnMeta(null, new SlowRow(0));

    Orca orca;
    Operator where = null;
    Map<ObjectName, Node> nodes = new LinkedHashMap<>();
    CursorMeta rawMeta;
    boolean forUpdate = false;
    List<Operator> groupBy;
    List<Operator> orderBy;
    List<Boolean> orderByDirections;
    List<OutputField> fields = new ArrayList<>();
    boolean isDistinct;
    Planner parent;
	Operator having;
	private GeneratorContext ctx;

	static {
		KEY.setColumnId(-1);
		KEY.setColumnName("*key");
		KEY.setType(DataType.blob());
		ROWID.setColumnId(0);
		ROWID.setColumnName("*rowid");
		ROWID.setType(DataType.longtype());
	}
	
    public Planner(GeneratorContext ctx) {
        this(ctx, (Planner)null);
    }
    
    public Planner(GeneratorContext ctx, Planner parent) {
    	this(ctx, parent != null ? parent.getRawMeta() : null);
    	this.parent = parent;
    }
    
    private Planner(GeneratorContext ctx, CursorMeta parentMeta) {
        this.orca = ctx.getOrca();
        this.ctx = ctx;
        this.rawMeta = new CursorMeta(parentMeta);
        if ((parentMeta != null) && (parentMeta.getColumnCount() > 0)) {
            Node node = new Node();
            node.alias = new ObjectName();
            for (FieldMeta i:parentMeta.getFields()) {
                node.fields.add((PlannerField)i);
            }
            node.isParent  = true;
            this.nodes.put(node.alias, node);
        }
    }
    
    public ObjectName addTable(String alias, TableMeta table, boolean isOuter) {
        Node node = new Node();
        node.table = table;
        if (alias == null) {
            node.alias = table.getObjectName();
        }
        else {
            node.alias = new ObjectName(null, alias);
        }
        node.isOuter = isOuter;
        this.nodes.put(node.alias, node);
        PlannerField keyField = new PlannerField(node, KEY);
        node.fields.add(keyField);
        this.rawMeta.addColumn(keyField);
        PlannerField rowidField = new PlannerField(node, ROWID);
        node.fields.add(rowidField);
        this.rawMeta.addColumn(rowidField);
        for (ColumnMeta column:table.getColumns()) {
            PlannerField field = new PlannerField(node, column);
            field.setSourceTable(table.getObjectName());
            node.fields.add(field);
            this.rawMeta.addColumn(field);
        }
        return node.alias;
    }
    
    public ObjectName addCursor(String name, CursorMaker maker) {
        Node node = new Node();
        node.maker = maker;
        node.alias = new ObjectName(null, name);
        node.isOuter = false;
        this.nodes.put(node.alias, node);
        for (FieldMeta i:maker.getCursorMeta().getFields()) {
            PlannerField field = new PlannerField(node, i);
            node.fields.add(field);
            this.rawMeta.addColumn(field);
        }
        return node.alias;
    }
    
    public void setWhere(Operator expr) {
        this.where = expr;
    }
    
    public void setHaving(Operator expr) {
        this.having = expr;
    }
    
    public void setGroupBy(List<Operator> exprs) {
        this.groupBy = exprs;
    }
    
    public void setOrderBy(List<Operator> exprs, List<Boolean> directions) {
        this.orderBy = exprs;
        this.orderByDirections = directions;
    }
    
    public void addJoinCondition(ObjectName alias, Operator expr, boolean outer) {
    	Node node = this.nodes.get(alias);
    	if (node == null) {
    		throw new OrcaException("alias is not found: " + alias);
    	}
    	node.joinCondition = expr;
    }
    
    public OutputField addOutputField(String name, Operator expr) {
    	adjustOpMatch(expr);
    	OutputField field = new OutputField(name, expr);
        this.fields.add(field);
        return field;
    }
    
    /**
     * inform OpMatch that it is not in where clause. this same expression in where clause returns a boolean but in
     * aggregator returns a float
     * 
     * @param expr
     */
    private void adjustOpMatch(Operator expr) {
		expr.visit(it -> {
			if (it instanceof OpMatch) {
				((OpMatch)it).setWhere(false);
			}
		});
	}

	public CursorMaker run() {
        analyze();
        Link path = build();
        
        // build join
        
        CursorMaker maker = buildJoin(path);
        
        // reindex fields. planner might adjust the order of participating tables;

        reindexFields(path);
        
        // where clause
        
        Operator condition = null;
        if (this.where != null) {
        	condition = this.where;
        }
        
        if (condition != null) {
            maker = new Filter(maker, condition, false, ctx.getNextMakerId());
        }
        
        // order by
        
    	maker = buildOrderby(maker);
        
        // group by 
        
        if (this.groupBy != null) {
        	if (this.groupBy.size() != 0) {
        		maker = new DumbGrouper(maker, this.groupBy, ctx.getNextMakerId());
        	}
        	else {
        		maker = new ThroughGrouper(maker);
        	}
        }
        
        // aggregation
        
        if (this.fields.size() > 0) {
            CursorMeta meta = new CursorMeta();
            List<Operator> exprs = new ArrayList<>();
            for (OutputField i:this.fields) {
                exprs.add(i.expr);
                FieldMeta field = new FieldMeta(i.name, i.expr.getReturnType());
                if (i.expr instanceof FieldValue) {
                	PlannerField pf = ((FieldValue)i.expr).getField();
                	field.setSourceTable(pf.getSourceTable());
                	field.setSourceColumnName(pf.getSourceName());
                	field.setTableAlias(pf.getTableAlias());
                }
                meta.addColumn(field);
            }
            maker = new Aggregator(maker, meta, exprs, this.ctx.getNextMakerId());
        }

        // group by post process
        
        if (this.groupBy != null) {
            maker = new GroupByPostProcesser(maker);
        }

        // having
        
        if (this.having != null) {
        	maker = new Filter(maker, this.having, false, ctx.getNextMakerId());
        }
        
        // distinct
        
        if (this.isDistinct) {
            maker = new DumbDistinctFilter(maker);
        }
        
        return maker;
    }
    
    private CursorMaker buildOrderby(CursorMaker maker) {
    	if (this.orderBy == null) {
    		return maker;
    	}
    	if (doesComplyOrder(maker, this.orderBy, this.orderByDirections)) {
    		return maker;
    	}
        maker = new DumbSorter(maker, this.orderBy, this.orderByDirections, ctx.getNextMakerId());
        return maker;
	}

	private boolean doesComplyOrder(RangeScannable maker,List<Operator> orderBy, List<Boolean> direction) {
		List<ColumnMeta> order = maker.getOrder();
		Vector from = maker.getFrom();
		Vector to = maker.getTo();
		int idx = 0;
		for (int i=0; i < order.size(); i++) {
			ColumnMeta column = order.get(i);
			Operator op = orderBy.get(idx); 
			
			// match the key column with orderby column
			
			if (op instanceof FieldValue) {
				FieldMeta field = ((FieldValue)op).getField();
				if ((field.getColumn() == column) && direction.get(idx)) {
					idx++;
					if (idx == orderBy.size()) {
						// all matched, perfect
						return true;
					}
					continue;
				}
			}
			
			// now if the range scan uses the same value at this spot, we can still continue
			// [1, 1] [1, 2] will work
			// [1, 1] [1, 2] [2, 1] will not work  
			
			if ((i < from.getValues().size()) && (i < to.getValues().size())) {
				if (from.getValues().get(i) == to.getValues().get(i)) {
					continue;
				}
			}
			return false;
		}
		return false;
	}
	
	private boolean doesComplyOrder(CursorMaker maker,List<Operator> orderBy, List<Boolean> direction) {
		if (maker instanceof ThroughGrouper) {
			return doesComplyOrder(((ThroughGrouper)maker).getUpstream(), orderBy, direction);
		}
		else if (maker instanceof Aggregator) {
			return doesComplyOrder(((Aggregator)maker).getUpstream(), orderBy, direction);
		}
		else if (maker instanceof TableRangeScan) {
			return doesComplyOrder((RangeScannable)maker, orderBy, direction);
		}
		else if (maker instanceof IndexRangeScan) {
			return doesComplyOrder((RangeScannable)maker, orderBy, direction);
		}
		return false;
	}

	private int indexFields(Link path) {
        if (path == null) {
            return 0;
        }
        if (path.to.isParent) {
        	return path.to.fields.size();
        }
        int pos = indexFields(path.previous);
        for (PlannerField i:path.to.fields) {
            i.index = pos++;
        }
        return pos;
    }

    // assign the field position
    private void reindexFields(Link path) {
        indexFields(path);
    }

    private CursorMaker buildJoin(Link path) {
        if (path.previous == null) {
            return path.maker;
        }
        CursorMaker makerLeft = buildJoin(path.previous);
        CursorMaker makerRight = path.maker;
        // join condition doesnt make sense if node order is changed
        Operator condition = path.to.isOuter ? path.to.joinCondition : buildAnd(path.to.joinCondition, path.join);
        NestedJoin join = new NestedJoin(makerLeft, makerRight, condition, path.to.isOuter, this.ctx.getNextMakerId());
        return join;
    }

    Link build() {
        Link link = build(null);
        if (link.getLevels() != nodes.size()) {
        	throw new CodingError();
        }
        return link;
    }
    
    Link build(Link previous) {
        Link link = null;
        for (Node node:this.nodes.values()) {
            // if node already been in the path, next
            
            if (previous != null) {
                if (previous.exists(node)) {
                    continue;
                }
            }
            
            // outer join node can't replace existing one
            
            if (node.isOuter && (link != null)) {
                continue;
            }
            
            // build the link
            
            Link result = build(previous, node);
            
            // keep the link with lowest score 
            
            if (link == null) {
                link = result;
            }
            else if (result.getScore() < link.getScore()) {
                link = result;
            }
            
            // don't score the rest if this is the parent query node
            
            if (node.isParent) {
                break;
            }
        }
        
        // end of nodes
        
        if (link == null) {
            return null;
        }
        
        // table level filter. only for those not used in seek/scan and the right operand is constant
        
        buildNodeFilters(link);
        
        // join conditions

        buildNodeJoinConditions(previous, link);
        
        // if not eof go deeper
        
        link.previous = previous;
        Link result = build(link);
        return (result != null) ? result : link;
    }
    
	private void buildNodeJoinConditions(Link previous, Link link) {
        if (previous != null) {
	        for (ColumnFilter i:link.to.getFilters()) {
	        	if (i.isConstant) {
	        		continue;
	        	}
	        	if (link.consumed.contains(i)) {
	        		continue;
	        	}
	        	if (checkColumnReference(previous, link.to, i.operand)) {
	        		link.join = buildAnd(link.join, i.source);
	        	}
	        }
        }
	}

	private Operator buildAnd(Operator x, Operator y) {
		if (x == null) {
			return y;
		}
		else if (y == null) {
			return x;
		}
		return new OpAnd(x, y);
	}
	
	private void buildNodeFilters(Link link) {
		// normal node
		
		if (!link.to.isUnion()) {
	        List<ColumnFilter> filters = new ArrayList<>();
	        for (ColumnFilter i:link.to.getFilters()) {
	        	if (i.isConstant && !link.consumed.contains(i)) {
	                filters.add(i);
	        	}
	        }
	        if (!filters.isEmpty()) {
	            createFilter(link, filters);
	        }
	        return;
		}
		
		// union node
		
		Operator where = null;
		for (Node i:link.to.unions) {
			Operator where_i = i.where;
			for (ColumnFilter j:i.getFilters()) {
				if (link.isUnion) {
		        	if (link.consumed.contains(i)) {
		        		continue;
		        	}
				}
				Operator op = createOperator(link.to, j);
				where_i = new OpAnd(where_i, op);
			}
			if (where == null) {
				where = where_i;
			}
			else {
				where = new OpOr(where, where_i);
			}
		}
		link.maker = new Filter(link.maker, where, ctx.getNextMakerId());
	}

	private Operator createOperator(Node node, ColumnFilter filter) {
    	PlannerField pf = new PlannerField(node, filter.field.field);
    	pf.column = filter.field.column;
    	pf.field = filter.field.field;
    	pf.index = node.findFieldPos(filter.field);
    	if (pf.index < 0) {
    		throw new CodingError();
    	}
        FieldValue cv = new FieldValue(pf);
        Operator op = null;
        if (filter.op == FilterOp.EQUAL) {
        	op = new OpEqual(cv, filter.operand);
        }
        else if (filter.op == FilterOp.EQUALNULL) {
        	op = new OpEqualNull(cv, filter.operand);
        }
        else if (filter.op == FilterOp.LARGER) {
        	op = new OpLarger(cv, filter.operand);
        }
        else if (filter.op == FilterOp.LARGEREQUAL) {
        	op = new OpLargerEqual(cv, filter.operand);
        }
        else if (filter.op == FilterOp.LESS) {
        	op = new OpLess(cv, filter.operand);
        }
        else if (filter.op == FilterOp.LESSEQUAL) {
        	op = new OpLessEqual(cv, filter.operand);
        }
        else if (filter.op == FilterOp.LIKE) {
        	op = new OpLike(cv, filter.operand);
        }
        else if (filter.op == FilterOp.INSELECT) {
        	op = filter.source;
        }
        else if (filter.op == FilterOp.INVALUES) {
        	op = filter.source;
        }
        else {
        	throw new NotImplementedException();
        }
        return op;
	}
	
	private void createFilter(Link link, List<ColumnFilter> filters) {
        // combine all filters into a AND
        
        Operator filter = null;
        for (ColumnFilter i:filters) {
            Operator op = createOperator(link.to, i);
            if (filter == null) {
            	filter = op;
            }
            else {
            	filter = new OpAnd(filter, op); 
            }
        }
        
        link.maker = new Filter(link.maker, filter, ctx.getNextMakerId());
    }

    Link build(Link previous, Node node) {
    	// not a union node
    	
    	if (!node.isUnion()) {
    		return build_(previous, node);
    	}
    	
    	// union node, only proceed is it produced two non table scan
    	
    	Link linkLeft = build_(previous, node.unions.get(0)); 
    	Link linkRight = build_(previous, node.unions.get(1));
    	if (!(linkLeft.maker instanceof TableScan) && !(linkRight.maker instanceof TableScan)) {
        	Link linkUnion = new Link(node);
        	linkUnion.isUnion = true;
        	linkUnion.maker = new Union(linkLeft.maker, linkRight.maker, true, ctx.getNextMakerId());
        	return linkUnion;
    	}
    	
    	// do the normal table scan
    	
    	Link link = build_(previous, node);
    	return link;
    }
    
    Link build_(Link previous, Node node) {
        Link link = null;
        
        // node is a record from outer query
        
        if (node.isParent) {
            link = new Link(node);
            link.maker = new MasterRecordCursorMaker(this.rawMeta.parent, ctx.getNextMakerId());
        }
        
        // try all keys/indexes

        if (link == null) {
        	link = tryKeys(previous, node);
        }
        
        // fall back to full table scan
        
        if (link == null) {
            link = new Link(node);
            if (node.table != null) {
                link.maker = new TableScan(node.table, ctx.getNextMakerId());
            }
            else if (node.maker != null) {
                link.maker = node.maker;
            }
            else {
                throw new CodingError();
            }
        }
        
        // alias support
        
        /*
        if ((node.table == null) || !node.alias.equals(node.table.getObjectName())) {
            link.maker = new Aliaser(node.alias.getTableName(), link.maker);
        }
        */
        
        // select for update support
        
        if (this.forUpdate) {
            GTable gtable = this.orca.getHumpback().getTable(node.table.getId());
            link.maker = new RecordLocker(link.maker, node.table, gtable);
        }
        
        // all done
        
        return link;
    }
    
    private Link tryKeys(Link previous, Node node) {
    	if (node.table == null) {
    		return null;
    	}
    	
        // no filters, can't do table range scan
        
        if (!hasFilters(node)) {
            return null;
        }
        
        // no primary key, can't do table range scan

        Link best = null;
        PrimaryKeyMeta pk = node.table.getPrimaryKey();
        if (pk != null) {
        	best = tryKey(previous, node, pk, true, false);
        }
        
    	// compare all indexes for the best match 
    	for (IndexMeta index:node.table.getIndexes()) {
    		Link link = tryKey(previous, node, index, index.isUnique(), index.isFullText());
    		if (link == null) {
    			continue;
    		}
    		if (best == null) {
    			best = link;
    			continue;
    		}
    		if (orderBy != null) {
        		if (doesComplyOrder(link.maker, orderBy, orderByDirections)) {
        			best = link;
        			break;
        		}
    		}
    		if (link.getScore() < best.getScore()) {
    			best = link;
    		}
    	}
		return best;
	}

	private Link tryKey(Link previous, Node node, RuleMeta<?> key, boolean isUnique, boolean isFullText) {
        // no filters, can't do table range scan
        
        if (!hasFilters(node)) {
            return null;
        }
        
        // match the key columns with the table filters one by one following the sequence defined in the key.
        
        Link link = new Link(node);
        List<ColumnMeta> columns = key.getColumns(node.table);
        if (columns.size() < 1) {
            return null;
        }
        Range range = new Range(columns.size());
        for (int i=0; i<columns.size(); i++) {
        	ColumnMeta column = columns.get(i);
            boolean found = false;
            for (ColumnFilter filter:node.getFilters()) {
                // is full text
                
                if (filter.op == FilterOp.MATCH) {
                	if (isFullText) {
                		link.maker = createFullTextScanner(node.table, (IndexMeta)key, filter);
                		if (link.maker == null) {
                			return null;
                		}
                		link.consumed.add(filter);
                		return link;
                	}
                	continue;
                }
                
                // is the same column?
                
                if (filter.field.column != column) {
                    continue;
                }
                
                // continue only if the column is renderable
                
                if (!checkColumnReference(previous, node, filter.operand)) {
                	continue;
                }

                // 
                
                found = true;
                if (range.addFilter(i, filter)) {
                	link.consumed.add(filter);                	
                }
            }
            if (!found) {
            	break;
            }
        }
        
        // create the cursor maker if it is still null
        
        link.maker = range.createMaker(node.table, key, ctx);
        return (link.maker == null) ? null : link;
	}

	private CursorMaker createFullTextScanner(TableMeta table, IndexMeta index, ColumnFilter filter) {
		OpMatch match = (OpMatch)filter.operand;
		if (match.getColumns().size() != index.getRuleColumns().size()) {
			return null;
		}
		for (FieldValue fv:match.getColumns()) {
			boolean found = false;
			for (RuleColumnMeta ruleColumn:index.getRuleColumns()) {
				if (fv.getField().getColumn().getId() == ruleColumn.getColumnId()) {
					found = true;
					break;
				}
			}
			if (!found) {
				return null;
			}
		}
		FullTextIndexMergeScan scan = new FullTextIndexMergeScan(table, index, ctx.getNextMakerId());
		scan.setQueryTerm(match.getAgainst());
		return scan;
	}

	@SuppressWarnings("unused")
	private boolean checkColumnReference(Node node, Link previous) {
    	for (ColumnFilter filter:node.getFilters()) {
    		if (!checkColumnReference(previous, node, filter.operand)) {
    			return false;
    		}
    	}
    	if (node.joinCondition != null) {
    		if (!checkColumnReference(previous, node, (BinaryOperator)node.joinCondition)) {
    			return false;
    		}
    	}
		return true;
	}

    /** is the expression calculable with given path */
    private boolean checkColumnReference(Link previous, Node node, Operator expr) {
    	if (expr instanceof OpInSelect) {
    		return true;
    	}
    	boolean[] valid = new boolean[1];
    	valid[0] = true;
        expr.visit(it -> {
        	if (!valid[0]) {
        		return;
        	}
            if (it instanceof FieldValue) {
                FieldValue cv = (FieldValue)it;
                if (node.findField(cv.getField()) != null) {
                    return;
                }
                if (previous != null) {
                    if (previous.findField(cv.getField()) != null) {
                        return;
                    }
                }
                valid[0] = false;
            }
        });
        return valid[0];
    }

    void analyze() {
        // analyze conditions
        
        if (this.where != null) {
            if (Analyzer.analyze(this, this.where, null)) {
            	this.where = null;
            }
        }
        
        // analyze join conditions
        
        for (Node i:this.nodes.values()) {
        	if (i.isOuter) {
        		Analyzer.analyze(this, i.joinCondition, i);
        	}
        	else if (i.joinCondition != null) {
        		if (Analyzer.analyze(this, i.joinCondition, null)) {
        			i.joinCondition = null;
        		}
        		else if (!isLocal(i, i.joinCondition)) {
        			// analyze will strip column filters out of the join condition. what's left in the join 
        			// condition should be local the node. otherwise we cannot proceed
        			throw new NotImplementedException();
        		}
        	}
        }
    }
    
    /**
     * check if the condition is local the specified node
     * @param i
     * @param joinCondition
     * @return
     */
	private boolean isLocal(Node node, Operator condition) {
		return Analyzer.isConstant(node, condition);
	}

	/**
     * fields coming from input
     * 
     * @return
     */
    public CursorMeta getRawMeta() {
        return this.rawMeta;
    }
    
    private boolean hasFilters(Node node) {
        if (node.getFilters().size() > 0) {
            return true;
        }
        else {
            return false;
        }
    }

    public boolean isEmpty() {
        return this.nodes.isEmpty();
    }

    /**
     * lock scanned records. this is used for SELECT FOR UPDATE
     * 
     * @param b
     */
    public void setForUpdate(boolean b) {
        this.forUpdate = b;
    }

    public void setDistinct(boolean b) {
        this.isDistinct = b;
    }

	public ObjectName addTableOrView(String alias, Object table, boolean isOuter) {
		ObjectName name = null;
		if (table instanceof TableMeta) {
			name = addTable(alias, (TableMeta)table, isOuter);
		}
		else if (table instanceof CursorMaker) {
			name = addCursor(alias, (CursorMaker)table);
		}
		else {
			throw new IllegalArgumentException();
		}
		return name;
	}

	public List<PlannerField> getFields() {
		List<PlannerField> list = new ArrayList<>();
		for (FieldMeta i:this.rawMeta.getFields()) {
			list.add((PlannerField)i);
		}
		return list;
	}
	
    public Object findTable(String name) {
    	ObjectName objname = new ObjectName(null, name.toLowerCase());
    	Node node = this.nodes.get(objname);
    	return node.table;
    }
    
    public Operator findOutputField(String name) {
    	for (OutputField i:this.fields) {
    		if (i.name.equalsIgnoreCase(name)) {
    			return i.expr;
    		}
    	}
    	return null;
    }
    
    public PlannerField findField(Predicate<FieldMeta> predicate) {
    	PlannerField result = null;
		for (Node i:this.nodes.values()) {
			if (i.isParent) {
				continue;
			}
			for (PlannerField j:i.fields) {
	            if (predicate.test(j)) {
	                if (result != null) {
	                    throw new OrcaException("Column is ambiguous: " + j);
	                }
	                result = j;
	            }
			}
		}
        if (result == null) {
        	if (this.parent != null) {
        		result = this.parent.findField(predicate);
        	}
        }
        return result;
    }
    
    public List<OutputField> getOutputFields() {
    	return this.fields;
    }
}
