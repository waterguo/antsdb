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
package com.antsdb.saltedfish.sql.planner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
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
import com.antsdb.saltedfish.sql.meta.OrcaTableType;
import com.antsdb.saltedfish.sql.meta.PrimaryKeyMeta;
import com.antsdb.saltedfish.sql.meta.RuleMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.AggregationFunction;
import com.antsdb.saltedfish.sql.vdm.Aggregator;
import com.antsdb.saltedfish.sql.vdm.BrutalGrouper;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.CursorPrimaryKeySeek;
import com.antsdb.saltedfish.sql.vdm.DumbDistinctFilter;
import com.antsdb.saltedfish.sql.vdm.DumbSorter;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;
import com.antsdb.saltedfish.sql.vdm.FieldValue;
import com.antsdb.saltedfish.sql.vdm.Filter;
import com.antsdb.saltedfish.sql.vdm.FullTextIndexMergeScan;
import com.antsdb.saltedfish.sql.vdm.FuncCount;
import com.antsdb.saltedfish.sql.vdm.FuncGroupConcat;
import com.antsdb.saltedfish.sql.vdm.FuncMax;
import com.antsdb.saltedfish.sql.vdm.FuncMin;
import com.antsdb.saltedfish.sql.vdm.FuncSum;
import com.antsdb.saltedfish.sql.vdm.IndexedTableScan;
import com.antsdb.saltedfish.sql.vdm.Limiter;
import com.antsdb.saltedfish.sql.vdm.MasterRecordCursorMaker;
import com.antsdb.saltedfish.sql.vdm.NestedJoin;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.OpAnd;
import com.antsdb.saltedfish.sql.vdm.OpEqual;
import com.antsdb.saltedfish.sql.vdm.OpMatch;
import com.antsdb.saltedfish.sql.vdm.OpOr;
import com.antsdb.saltedfish.sql.vdm.Operator;
import com.antsdb.saltedfish.sql.vdm.Ordered;
import com.antsdb.saltedfish.sql.vdm.RecordLocker;
import com.antsdb.saltedfish.sql.vdm.RowKeyValue;
import com.antsdb.saltedfish.sql.vdm.TableScan;
import com.antsdb.saltedfish.sql.vdm.AllInOneGrouper;
import com.antsdb.saltedfish.sql.vdm.ToString;
import com.antsdb.saltedfish.sql.vdm.Union;
import com.antsdb.saltedfish.sql.vdm.Vector;
import com.antsdb.saltedfish.sql.vdm.View;
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
    private Node last;
    private Node current;
    private boolean noCache = false;
    private Analyzer analyzer = new Analyzer();
    private long offset;
    private long count;
    private CursorMeta outputMeta = new CursorMeta();
    private boolean freeze = false;
    private ArrayList<AggregationFunction> aggregates;

    static {
        KEY.setColumnId(-1);
        KEY.setColumnName("*key");
        KEY.setType(DataType.varchar());
        ROWID.setColumnId(0);
        ROWID.setColumnName("*rowid");
        ROWID.setType(DataType.binary());
    }

    public Planner(GeneratorContext ctx) {
        this(ctx, (Planner) null);
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
            for (FieldMeta i : parentMeta.getFields()) {
                node.fields.add((PlannerField) i);
            }
            node.isParent = true;
            this.nodes.put(node.alias, node);
        }
    }

    public ObjectName addTable(String alias, TableMeta table, boolean left, boolean isOuter) {
        if (this.freeze) {
            throw new IllegalArgumentException("freezed");
        }
        Node node = new Node();
        node.table = table;
        if (alias == null) {
            node.alias = table.getObjectName();
        }
        else {
            node.alias = new ObjectName(null, alias);
        }
        if (left) {
            node.isOuter = isOuter;
        }
        else {
            // right join must be an outer join
            this.current.isOuter = true;
            node.isOuter = false;
        }
        this.nodes.put(node.alias, node);
        PlannerField keyField = new PlannerField(node, KEY);
        node.fields.add(keyField);
        this.rawMeta.addColumn(keyField);
        PlannerField rowidField = new PlannerField(node, ROWID);
        node.fields.add(rowidField);
        this.rawMeta.addColumn(rowidField);
        PrimaryKeyMeta pkmeta = table.getPrimaryKey();
        for (ColumnMeta column : table.getColumns()) {
            PlannerField field = new PlannerField(node, column);
            field.setSourceTable(table.getObjectName());
            if (pkmeta != null && pkmeta.isKeyColumn(column)) {
                field.setKeyColumn(true);
            }
            node.fields.add(field);
            this.rawMeta.addColumn(field);
        }
        this.last = this.current;
        this.current = node;
        return node.alias;
    }

    private void addCursor(Node node, CursorMeta meta, ObjectName sourceName, boolean left, boolean isOuter) {
        if (left) {
            node.isOuter = isOuter;
        }
        else {
            // right join must be an outer join
            this.current.isOuter = true;
            node.isOuter = false;
        }
        this.nodes.put(node.alias, node);
        for (FieldMeta i : meta.getFields()) {
            PlannerField field = new PlannerField(node, i);
            field.setSourceTable(sourceName);
            field.index = this.rawMeta.getColumnCount();
            node.fields.add(field);
            this.rawMeta.addColumn(field);
        }
        this.last = this.current;
        this.current = node;
    }
    
    public ObjectName addCursor(String alias, Planner planner, boolean left, boolean isOuter) {
        if (this.freeze) {
            throw new IllegalArgumentException("freezed");
        }
        Node node = new Node();
        node.planner = planner;
        node.alias = new ObjectName(null, alias);
        addCursor(node, planner.getOutputMeta(), null, left, isOuter);
        return node.alias;
    }
    
    public ObjectName addCursor(String alias, CursorMaker maker, boolean left, boolean isOuter) {
        if (this.freeze) {
            throw new IllegalArgumentException("freezed");
        }
        View view = (maker instanceof View) ? (View)maker : null;
        Node node = new Node();
        node.maker = maker;
        ObjectName sourceName = (view != null) ? view.getName() : null;
        if ((alias == null) && (view != null)) {
            node.alias = view.getName();
        }
        else {
            node.alias = new ObjectName(null, alias);
        }
        addCursor(node, maker.getCursorMeta(), sourceName, left, isOuter);
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

    public void addJoinCondition(ObjectName alias, Operator expr, boolean left) {
        Node node = null;
        if (left) {
            node = this.nodes.get(alias);
            if (node == null) {
                throw new OrcaException("alias is not found: " + alias);
            }
        }
        else {
            node = this.last;
        }
        node.joinCondition = expr;
        if (node.planner != null) {
            pushDown(node.planner, expr);
        }
    }

    /**
     * push down the provided condition to subquery so it can take advantage of indexes potentially. 
     * 
     * @param planner subquery
     * @param expr condition
     */
    private void pushDown(Planner subquery, Operator expr) {
        if (!(expr instanceof OpEqual)) {
            // for now, we only handle a single =
            return;
        }
        OpEqual equal = (OpEqual)expr;
        if (!(equal.left instanceof FieldValue) || !(equal.right instanceof FieldValue)) {
            // both operands of the = must be field name
            return;
        }
        FieldValue x = findField(this, subquery, (FieldValue)equal.left);
        FieldValue y = findField(this, subquery, (FieldValue)equal.right);
        if ((x == null) || (y == null)) return;
        OpEqual z = new OpEqual(x, y);
        if (subquery.where == null) {
            subquery.where = z;
        }
        else {
            subquery.where = new OpAnd(subquery.where, z);
        }
    }

    private FieldValue findField(Planner planner, Planner subquery, FieldValue fv) {
        if (fv.getField().owner.planner == subquery) {
            for (int i=0; i<subquery.getOutputMeta().getColumnCount(); i++) {
                if (subquery.getOutputMeta().getColumn(i) == fv.getField().field) {
                    if (subquery.getOutputFields().get(i).expr instanceof FieldValue) {
                        return (FieldValue)subquery.getOutputFields().get(i).expr;
                    }
                }
            }
        }
        else {
            PlannerField result = subquery.findField(it->{
                return it.getColumn() == fv.getField().getColumn();
            });
            if (result != null) {
                return new FieldValue(result);
            }
        }
        return null;
    }

    public OutputField addOutputField(String name, Operator expr) {
        // remove the system column prefix if it is wanted in the query result. it is because 
        // Helper.getColumnCount() will hide the columns start with '*'
        if (name.startsWith("*")) {
            name = name.substring(1);
            if (name.equals("key")) {
                // pure hacks
                expr = new ToString(expr);
            }
        }
        
        adjustOpMatch(expr);
        OutputField field = new OutputField(name, expr);
        this.fields.add(field);
        FieldMeta fm = new FieldMeta(field.name, field.expr.getReturnType());
        if (field.expr instanceof FieldValue) {
            PlannerField pf = ((FieldValue)field.expr).getField();
            fm.setSourceTable(pf.getSourceTable());
            fm.setSourceColumnName(pf.getSourceName());
            fm.setTableAlias(pf.getTableAlias());
            fm.setColumn(pf.column);
            fm.setKeyColumn(pf.isKeyColumn());
        }
        this.outputMeta.addColumn(fm);
        findAggregationFunction(expr);
        return field;
    }

    private void findAggregationFunction(Operator expr) {
        expr.visit((Operator i)->{
           if (i instanceof AggregationFunction) {
               if (this.aggregates == null) {
                   this.aggregates = new ArrayList<>();
               }
               this.aggregates.add((AggregationFunction) i);
           }
        });
    }
    /**
     * inform OpMatch that it is not in where clause. this same expression in
     * where clause returns a boolean but in aggregator returns a float
     * 
     * @param expr
     */
    private void adjustOpMatch(Operator expr) {
        expr.visit(it -> {
            if (it instanceof OpMatch) {
                ((OpMatch) it).setWhere(false);
            }
        });
    }

    public CursorMeta getOutputMeta() {
        return this.outputMeta;
    }
    
    public CursorMaker run() {
        return run(0);
    }
    
    /**
     * 
     * @param isParentAvailable if parent is available in a subquery. parent query could be unavailable if the subquery 
     * is the first one to execute. 
     * @return
     */
    public CursorMaker run(int parentWidth) {
        if ((this.groupBy == null) && scanAggregationFunctions(getOutputFields())) {
            this.groupBy = Collections.emptyList();
        }
        
        analyze();
        
        // optimization
        leftJoinOptimize();
        
        // build query path
        Link path = build(parentWidth);

        // build join
        CursorMaker maker = buildJoin(path);

        // reindex fields. planner might adjust the order of participating
        // tables;
        reindexFields(path);

        // where clause
        Operator condition = null;
        if (this.where != null) {
            condition = this.where;
        }

        if (condition != null) {
            maker = new Filter(maker, condition, false, ctx.getNextMakerId());
        }

        // group by
        CursorMaker grouper = buildGroupBy(maker, path);
        maker = grouper;
        
        // add order by fields
        int posOrderBy = addOrderByFields(maker);
        
        // aggregation
        if (this.fields.size() > 0) {
            List<Operator> exprs = new ArrayList<>();
            for (OutputField i : this.fields) {
                exprs.add(i.expr);
            }
            if (grouper instanceof BrutalGrouper) {
                ((BrutalGrouper)grouper).setOutput(this.outputMeta, exprs);
            }
            else {
                maker = new Aggregator(maker, this.outputMeta, exprs, this.ctx.getNextMakerId());
            }
        }

        // order by
        if (posOrderBy > 0) {
            maker = new DumbSorter(maker, this.orderBy, this.orderByDirections, ctx.getNextMakerId(), posOrderBy);
        }

        // having
        if (this.having != null) {
            maker = new Filter(maker, this.having, false, ctx.getNextMakerId());
        }

        // distinct
        if (this.isDistinct) {
            maker = new DumbDistinctFilter(maker);
        }

        // limit
        if ((this.count != 0) || (this.offset != 0)) {
            maker = new Limiter(maker, this.offset, this.count);
        }
        
        return maker;
    }

    // some of the left joins can be converted to cross joins
    private void leftJoinOptimize() {
        for (Node i:this.nodes.values()) {
            if (!i.isOuter) continue;
            if (i.union.size() != 1) {
                // lets dont deal too much complexity for now
                continue;
            }
            RowSet rs = i.union.get(0);
            for (ColumnFilter j:rs.conditions) {
                if (!j.isJoin) {
                    i.isOuter = false;
                    break;
                }
            }
        }
    }

    private CursorMaker buildGroupBy(CursorMaker maker, Link path) {
        if (this.groupBy == null) {
            return maker;
        }
        if (this.groupBy.size() == 0) {
            // group the enter result set. happens when query has aggregation function but no group by clause
            maker = new BrutalGrouper(maker, null, this.aggregates, ctx.getNextMakerId());
            return maker;
        }
        List<PlannerField> groupKey = getGroupKey(this.groupBy);
        if (path.isUnique(groupKey)) {
            // result is unique by the key. no grouping is needed
            return maker;
        }
        // maker = new DumbGrouper(maker, this.groupBy, ctx.getNextMakerId());
        maker = new BrutalGrouper(maker, this.groupBy, this.aggregates, ctx.getNextMakerId());
        return maker;
    }

    @SuppressWarnings("unused")
    private List<PlannerField> getGroupKey(List<Operator> expr) {
        List<PlannerField> result = new ArrayList<>();
        for (Operator op : expr) {
            if (!(op instanceof FieldValue)) {
                return null;
            }
            FieldValue fv = (FieldValue) op;
            result.add(fv.getField());
        }
        return result;
    }

    private int addOrderByFields(CursorMaker maker) {
        if (this.orderBy == null) {
            return 0;
        }
        if (hasImplicitGroupBy(maker)) {
            // skip order by if there is implicit group by clause
            return 0;
        }
        List<SortKey> sortKey = toSortKey(this.orderBy, this.orderByDirections);
        if (sortKey != null) {
            if (maker.setSortingOrder(sortKey)) {
                return 0;
            }
        }
        int result = this.fields.size();
        for (Operator i:this.orderBy) {
            addOutputField("#", i);
        }
        return result;
    }
    
    private List<SortKey> toSortKey(List<Operator> ops, List<Boolean> directions) {
        List<SortKey> result = new ArrayList<>();
        for (int i=0; i<ops.size(); i++) {
            Operator op = ops.get(i);
            if (!(op instanceof FieldValue)) {
                return null;
            }
            FieldMeta field = ((FieldValue)op).getField();
            ColumnMeta column = field.getColumn();
            if (column == null) {
                return null;
            }
            SortKey key = new SortKey(column, directions.get(i));
            result.add(key);
        }
        return result;
    }
    
    /**
     * if the query has aggregation functions but no groupby
     * 
     * @param maker
     * @return
     */
    private boolean hasImplicitGroupBy(CursorMaker maker) {
        return this.groupBy != null && (this.groupBy.size() == 0);
    }

    private boolean doesComplyOrder(Ordered maker, List<Operator> orderBy, List<Boolean> direction) {
        List<ColumnMeta> order = maker.getOrder();
        int idx = 0;
        for (int i = 0; i < order.size(); i++) {
            ColumnMeta column = order.get(i);
            Operator op = orderBy.get(idx);

            // match the key column with orderby column
            if (op instanceof FieldValue) {
                FieldMeta field = ((FieldValue) op).getField();
                if (field.getColumn() == column) {
                    if (direction == null || direction.get(idx)) {
                        idx++;
                        if (idx == orderBy.size()) {
                            // all matched, perfect
                            return true;
                        }
                        continue;
                    }
                }
            }
            return false;
        }
        return false;
    }
    
    private boolean doesComplyOrder(CursorMaker maker, List<Operator> orderBy, List<Boolean> direction) {
        if (maker instanceof AllInOneGrouper) {
            return doesComplyOrder(((AllInOneGrouper) maker).getUpstream(), orderBy, direction);
        }
        else if (maker instanceof Aggregator) {
            return doesComplyOrder(((Aggregator) maker).getUpstream(), orderBy, direction);
        }
        else if (maker instanceof Ordered) {
            return doesComplyOrder((Ordered) maker, orderBy, direction);
        }
        else if (maker instanceof NestedJoin) {
            return doesComplyOrder(((NestedJoin)maker).getLeft(), orderBy, direction);
        }
        return false;
    }

    private int indexFields(Link path) {
        if (path == null) {
            return 0;
        }
        if (path.to.isParent) {
            return path.to.getWidth();
        }
        int pos = indexFields(path.previous);
        for (int i=0; i<path.to.fields.size(); i++) {
            PlannerField ii = path.to.fields.get(i);
            ii.noCache = this.noCache;
            if (ii.column != null) {
                ii.index = pos + ii.column.getColumnId() + 1;
            }
            else {
                ii.index = pos + i;
            }
        }
        return pos + path.to.fields.size();
    }

    // assign the field position
    private void reindexFields(Link path) {
        indexFields(path);
    }

    private CursorMaker buildJoin(Link path) {
        if (path.previous == null) {
            if (path.join != null) {
                // need to apply the join conditions when there is no join
                // @see TestPlanner.testMultipleInSelect
                path.maker = new Filter(path.maker, path.join, this.ctx.getNextMakerId());
            }
            return path.maker;
        }
        CursorMaker makerLeft = buildJoin(path.previous);
        CursorMaker makerRight = path.maker;
        // join condition doesnt make sense if node order is changed
        Operator condition = path.to.isOuter ? path.to.joinCondition : buildAnd(path.to.joinCondition, path.join);
        NestedJoin join = new NestedJoin(
                makerLeft, 
                makerRight,
                path.previous.getWidth(),
                path.to.getWidth(),
                condition, 
                path.to.isOuter, 
                this.ctx.getNextMakerId());
        return join;
    }

    Link build(int parentWidth) {
        Link link = build(null, parentWidth);
        if (link.getLevels() != nodes.size()) {
            if (this.parent != null) {
                if (link.getLevels() != nodes.size()-1) {
                    // levels could be less than number of nodes by 1 if the parent is not referenced
                    throw new CodingError();
                }
            }
        }
        return link;
    }

    Link build(Link previous, int parentWidth) {
        Link link = null;
        for (Node node : this.nodes.values()) {
            // skip the parent node if we are building the driver
            if (node.isParent && parentWidth==0) continue;
            
            // skip the parent node if we are building the driver
            if (node.isParent && parentWidth==0) continue;
            
            // skip the parent node if it is not used
            if (node.isParent && !node.isUsed) {
                continue;
            }
            
            // if node already been in the path, next
            if (previous != null) {
                if (previous.exists(node)) {
                    continue;
                }
            }

            // outer join node cant start the query
            if (node.isOuter && (previous == null)) {
                continue;
            }
            
            // outer join node can't replace existing one
            if (node.isOuter && (link != null)) {
                continue;
            }

            // build the link
            Link result = build(previous, node, parentWidth);

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

        // if not eof go deeper
        link.previous = previous;
        Link result = build(link, parentWidth);
        return (result != null) ? result : link;
    }

    /**
     * build the expression that can't be used at table level
     * @see TestPlanner.
     * @param previous
     * @param link
     * @param filters
     */
    private void buildNodeJoinConditions(Link previous, Link link, RowSet tqs) {
        for (ColumnFilter i : tqs.conditions) {
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

    private Operator buildOr(Operator x, Operator y) {
        if (x == null) {
            return y;
        }
        else if (y == null) {
            return x;
        }
        return new OpOr(x, y);
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

    private Operator buildNodeFilters(Link link, RowSet tqs) {
        Operator result = null;
        for (ColumnFilter i : tqs.conditions) {
            if (!i.isConstant) continue;
            if (link.consumed.contains(i)) continue;
            if (result == null) {
                result = createOperator(link.to, i);
            }
            else {
                result = new OpAnd(result, createOperator(link.to, i));
            }
        }
        return result;
    }

    private Operator createOperator(Node owner, ColumnFilter filter) {
        filter.source.visit((Operator it)->{
            if (it instanceof FieldValue) {
                FieldValue fv = (FieldValue)it;
                int pos = owner.findFieldPos(fv.getField());
                if (pos >= 0) {
                    PlannerField pf = new PlannerField(owner, fv.getField());
                    pf.column = fv.getField().column;
                    pf.field = fv.getField().field;
                    pf.index = pos;
                    fv.set(pf);
                }
            }
        });
        return filter.source;
        /* old implementation, wait a little before deleting
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
            op = new OpEqual(cv, (Operator)filter.operand);
        }
        else if (filter.op == FilterOp.EQUALNULL) {
            op = new OpEqualNull(cv, (Operator)filter.operand);
        }
        else if (filter.op == FilterOp.LARGER) {
            op = new OpLarger(cv, (Operator)filter.operand);
        }
        else if (filter.op == FilterOp.LARGEREQUAL) {
            op = new OpLargerEqual(cv, (Operator)filter.operand);
        }
        else if (filter.op == FilterOp.LESS) {
            op = new OpLess(cv, (Operator)filter.operand);
        }
        else if (filter.op == FilterOp.LESSEQUAL) {
            op = new OpLessEqual(cv, (Operator)filter.operand);
        }
        else if (filter.op == FilterOp.LIKE) {
            op = new OpLike(cv, (Operator)filter.operand);
        }
        else if (filter.op == FilterOp.INSELECT) {
            op = new OpInSelect(cv, (CursorMaker)filter.operand);
        }
        else if (filter.op == FilterOp.INVALUES) {
            op = new OpInValues(cv, (List<Operator>)filter.operand);
        }
        else {
            throw new NotImplementedException();
        }
        return op;
        */
    }

    /* deprecated wait a little before deleting
    private Operator createFilterExpression(Link link, List<ColumnFilter> filters) {
        // combine all filters into a AND

        Operator filter = null;
        for (ColumnFilter i : filters) {
            Operator op = createOperator(link.to, i);
            if (filter == null) {
                filter = op;
            }
            else {
                filter = new OpAnd(filter, op);
            }
        }
        return filter;
    }
    */

    Link buildTableScan(Link previous, Node node, int parentWidth) {
        Link result = build_(previous, node, null, parentWidth);
        Operator filter = null;
        for (RowSet i:node.union) {
            filter = buildOr(filter, buildNodeFilters(result, i));
            buildNodeJoinConditions(previous, result, i);
        }
        if (filter != null) {
            result.maker = new Filter(result.maker, filter, ctx.getNextMakerId());
        }
        return result;
    }
    
    CursorMaker createTableScan(Node node, Link previous) {
        if (node.table != null) {
            TableScan ts = new TableScan(node.table, ctx.getNextMakerId());
            ts.setNoCache(this.noCache);
            return ts;
        }
        else {
            return node.getCursorMaker(previous != null ? previous.getWidth() : 0);
        }
    }
    
    Link build(Link previous, Node node, int parentWidth) {
        Link result = null;
        
        // no columns filters found, just build the full table scan
        if (node.union.size() <= 0) {
            return build_(previous, node, null, parentWidth);
        }
        
        // union node, only proceed is it produced two non table scan
        List<Link> union = new ArrayList<>();
        boolean foundFullTableScan = false;
        for (RowSet i:node.union) {
            if (i.conditions.isEmpty()) {
                foundFullTableScan = true;
            }
        }
        if (!foundFullTableScan) {
            for (RowSet i:node.union) {
                // skip if this is a full table scan, we will produce full table scan anyway
                if (i.conditions.isEmpty()) {
                    continue;
                }
                Link ii = build_(previous, node, i, parentWidth);
                
                // table level filter. only for those not used in seek/scan and the
                // right operand is constant
                Operator filter = buildNodeFilters(ii, i);
                if (filter != null) {
                    ii.maker = new Filter(ii.maker, filter, ctx.getNextMakerId());
                }
    
                // join conditions
                buildNodeJoinConditions(previous, ii, i);
                
                // build  union
                union.add(ii);
            }
        }

        // find the better plan between full table scan and union
        Link tablescan = buildTableScan(previous, node, parentWidth);
        if (union.isEmpty() || tablescan.getScore() <= getScore(union)) {
            result = tablescan;
        }
        else {
            result = buildUnion(union);
            buildNatralOrder(previous, result);
        }
            
        return result;
    }

    private void buildNatralOrder(Link previous, Link result) {
        // if this is the driving table, we need to apply the row key order
        if (previous != null) {
            return;
        }
        if (this.orderBy != null) {
            return;
        }
        if (!(result.maker instanceof Union) && !(result.maker instanceof CursorPrimaryKeySeek)) {
            return;
        }
        result.maker = new DumbSorter(result.maker, new RowKeyValue(), true, ctx.getNextMakerId());
    }

    private float getScore(List<Link> union) {
        float result = 0;
        for (Link i:union) {
            result += i.getScore();
        }
        return result;
    }

    Link buildUnion(List<Link> union) {
        Link result = null;
        for (Link i:union) {
            if (result == null) {
                result = i;
            }
            else {
                Link merge = new Link(i.to);
                merge.isUnion = true;
                Link x = result;
                Link y = i;
                // we cant union join conditions. if there is any, create a filter
                // @see TestTopkaQueries.testSlowQuery2()
                if (x.join != null) {
                    x.maker = new Filter(x.maker, x.join, ctx.getNextMakerId());
                }
                if (y.join != null) {
                    y.maker = new Filter(y.maker, y.join, ctx.getNextMakerId());
                }
                merge.maker = new Union(x.maker, y.maker, false, ctx.getNextMakerId());
                result = merge;
            }
        }
        return result;
    }
    
    Link build_(Link previous, Node node, RowSet tqs, int parentWidth) {
        Link link = null;

        // node is a record from outer query
        if (node.isParent) {
            link = new Link(node);
            link.width = parentWidth;
            link.maker = new MasterRecordCursorMaker(this.rawMeta.parent, ctx.getNextMakerId());
        }

        // try all keys/indexes
        if (link == null) {
            link = tryKeys(previous, node, tqs);
        }

        // try indexed table scan
        if (link == null) {
            link = tryIndexTableScan(previous, node, tqs);
        }
        
        // fall back to full table scan
        if (link == null) {
            link = new Link(node);
            link.maker = createTableScan(node, previous);
        }

        // alias support
        /*
         * if ((node.table == null) ||
         * !node.alias.equals(node.table.getObjectName())) { link.maker = new
         * Aliaser(node.alias.getTableName(), link.maker); }
         */

        // select for update support
        if (this.forUpdate) {
            GTable gtable = this.orca.getHumpback().getTable(node.table.getHtableId());
            link.maker = new RecordLocker(link.maker, node.table, gtable);
        }

        // all done
        return link;
    }

    /*
     * build a just in time index for table scan in order to speed up joins
     */
    private Link tryIndexTableScan(Link previous, Node node, RowSet rs) {
        // on filters
        if (rs == null) return null;
        
        // not applicable if this is the driver table
        if (previous == null) return null;
        
        // not applicable if there is no join condition
        List<ColumnFilter> qualified = new ArrayList<>();
        List<Operator> values = new ArrayList<>();
        for (ColumnFilter i:rs.conditions) {
            if (i.op != FilterOp.EQUAL && i.op != FilterOp.EQUALNULL) continue;
            if (!checkColumnReference(previous, node, i.operand)) continue;
            qualified.add(i);
            values.add((Operator)i.operand);
        }
        if (qualified.size() <= 0) return null;
        
        // done finish up
        int[] keyFields = new int[qualified.size()];
        for (int i=0; i<keyFields.length; i++) {
            keyFields[i] = node.findFieldPos(qualified.get(i).field);
        }
        Vector v = new Vector(values, true, true);
        CursorMaker upstream = createTableScan(node, previous);
        Link result = new Link(node);
        result.consumed.addAll(qualified);
        result.maker = new IndexedTableScan(upstream, keyFields, v, ctx.getNextMakerId());
        return result;
    }

    private Link tryKeys(Link previous, Node node, RowSet tqs) {
        if (node.table == null) {
            return null;
        }

        // no filters, can't do table range scan
        if (tqs == null) {
            return null;
        }
        List<ColumnFilter> filters = tqs.conditions;
        if (filters.size() <= 0) {
            return null;
        }

        // no primary key, can't do table range scan
        Link best = null;
        PrimaryKeyMeta pk = node.table.getPrimaryKey();
        if (pk != null) {
            best = tryKey(previous, node, filters, pk, true, false);
        }

        // compare all indexes for the best match
        for (IndexMeta index : node.table.getIndexes()) {
            Link link = tryKey(previous, node, filters, index, index.isUnique(), index.isFullText());
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
            else if (link.getScore() == best.getScore()) {
                // if multiple indexes can be applied, we pick the one that matches first in the where clause
                if (link.pos < best.pos) {
                    best = link;
                }
            }
        }
        return best;
    }

    private Link tryKey(Link previous, 
                        Node node, 
                        List<ColumnFilter> filters, 
                        RuleMeta<?> key, 
                        boolean isUnique, 
                        boolean isFullText) {
        // no filters, can't do table range scan
        if (filters.size() <= 0) {
            return null;
        }

        // match the key columns with the table filters one by one following the
        // sequence defined in the key.
        Link link = new Link(node);
        List<ColumnMeta> columns = key.getColumns(node.table);
        if (columns.size() < 1) {
            return null;
        }
        Range range = new Range(columns.size());
        int filterPos = -1;
        for (int i = 0; i < columns.size(); i++) {
            ColumnMeta column = columns.get(i);
            boolean found = false;
            for (int j=0; j<filters.size(); j++) {
                ColumnFilter filter = filters.get(j);
                // is full text
                if (filter.op == FilterOp.MATCH) {
                    if (isFullText) {
                        link.maker = createFullTextScanner(node.table, (IndexMeta) key, filter);
                        if (link.maker == null) {
                            return null;
                        }
                        link.consumed.add(filter);
                        return link;
                    }
                    continue;
                }

                // is the same column?
                if (filter.field == null || filter.field.column != column) {
                    continue;
                }

                // continue only if the column is renderable
                if (!checkColumnReference(previous, node, filter.operand)) {
                    continue;
                }

                //
                found = true;
                if (filterPos == -1) {
                    filterPos = j;
                }
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
        link.pos = filterPos;
        return (link.maker == null) ? null : link;
    }

    private CursorMaker createFullTextScanner(TableMeta table, IndexMeta index, ColumnFilter filter) {
        OpMatch match = (OpMatch) filter.operand;
        if (match.getColumns().size() != index.getRuleColumns().length) {
            return null;
        }
        for (FieldValue fv : match.getColumns()) {
            boolean found = false;
            for (int ruleColumn : index.getRuleColumns()) {
                if (fv.getField().getColumn().getId() == ruleColumn) {
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

    /** is the expression calculable with given path */
    private boolean checkColumnReference(Link previous, Node node, Object expr) {
        if (expr instanceof CursorMaker) {
            return true;
        }
        else if (expr instanceof List<?>) {
            for (Object i:(List<?>)expr) {
                if (!checkColumnReference(previous, node, i)) {
                    return false;
                }
            }
            return true;
        }
        else if (expr instanceof Operator) {
            boolean[] valid = new boolean[1];
            valid[0] = true;
            ((Operator)expr).visit(it -> {
                if (!valid[0]) {
                    return;
                }
                if (it instanceof FieldValue) {
                    FieldValue cv = (FieldValue) it;
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
        else {
            throw new IllegalArgumentException();
        }
    }

    public void analyze() {
        // analyze conditions
        if (this.where != null) {
            if (this.analyzer.analyzeWhere(this, this.where)) {
                this.where = null;
            }
        }

        // analyze join conditions
        for (Node i : this.nodes.values()) {
            if (i.isOuter) {
                this.analyzer.analyzeJoin(this, i.joinCondition, i);
            }
            else if (i.joinCondition != null) {
                if (this.analyzer.analyzeJoin(this, i.joinCondition, null)) {
                    i.joinCondition = null;
                }
                else if (!isLocal(i, i.joinCondition)) {
                    // analyze will strip column filters out of the join
                    // condition. what's left in the join
                    // condition should be local the node. otherwise we cannot
                    // proceed
                    throw new NotImplementedException();
                }
            }
        }
    }

    /**
     * check if the condition is local the specified node
     * 
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

    public boolean isEmpty() {
        if (this.parent == null) {
            return this.nodes.isEmpty();
        }
        else {
            return this.nodes.size() <= 1;
        }
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

    public ObjectName addTableOrView(String alias, Object table, boolean left, boolean isOuter) {
        ObjectName name = null;
        if (table instanceof TableMeta) {
            TableMeta meta = (TableMeta)table;
            if (meta.getType() == OrcaTableType.VIEW) {
                CursorMaker maker = (CursorMaker)this.ctx.getSession().parse(this.ctx, meta.getViewSql()).getRoot();
                for (int i=0; i<meta.getColumns().size(); i++) {
                    maker.getCursorMeta().getColumns().get(i).setName(meta.getColumns().get(i).getColumnName());
                }
                name = addCursor(alias==null ? meta.getTableName() : alias, maker, left, isOuter);
            }
            else {
                name = addTable(alias, meta, left, isOuter);
            }
        }
        else if (table instanceof CursorMaker) {
            name = addCursor(alias, (CursorMaker) table, left, isOuter);
        }
        else {
            throw new IllegalArgumentException();
        }
        return name;
    }

    public List<PlannerField> getFields() {
        List<PlannerField> list = new ArrayList<>();
        for (FieldMeta i : this.rawMeta.getFields()) {
            list.add((PlannerField) i);
        }
        return list;
    }

    public Object findTable(String name) {
        ObjectName objname = new ObjectName(null, name.toLowerCase());
        Node node = this.nodes.get(objname);
        return node.table;
    }

    public Operator findOutputField(String name) {
        for (OutputField i : this.fields) {
            if (i.name.equalsIgnoreCase(name)) {
                return i.expr;
            }
        }
        return null;
    }

    private PlannerField findFrield(Predicate<FieldMeta> predicate, boolean inParent) {
        PlannerField result = null;
        for (Node i : this.nodes.values()) {
            boolean b = i.isParent ^ inParent; 
            if (!b) {
                for (PlannerField j : i.fields) {
                    if (predicate.test(j)) {
                        if (result != null) {
                            throw new OrcaException("Column is ambiguous: " + j);
                        }
                        result = j;
                    }
                }
            }
        }
        return result;
    }
    
    public PlannerField findField(Predicate<FieldMeta> predicate) {
        PlannerField result = null;
        result = findFrield(predicate, false);
        if (result == null) {
            if (this.parent != null) {
                result = findFrield(predicate, true);
                if (result != null) {
                    for (Node i : this.nodes.values()) {
                        if (i.isParent) {
                            i.isUsed = true;
                        }
                    }
                }
            }
        }
        return result;
    }

    public List<OutputField> getOutputFields() {
        return this.fields;
    }
    
    public void setNoCache(boolean value) {
        this.noCache = value;
    }
    
    private static boolean scanAggregationFunctions(List<OutputField> list) {
        AtomicBoolean result = new AtomicBoolean(false);
        for (OutputField i:list) {
            i.getExpr().visit(it -> {
                if (it instanceof FuncCount) {
                    result.set(true);
                }
                else if (it instanceof FuncMax) {
                    result.set(true);
                }
                else if (it instanceof FuncMin) {
                    result.set(true);
                }
                else if (it instanceof FuncSum) {
                    result.set(true);
                }
                else if (it instanceof FuncCount) {
                    result.set(true);
                }
                else if (it instanceof FuncGroupConcat) {
                    result.set(true);
                }
            });
        }
        return result.get();
    }
    
    public Map<ObjectName, Node> getNodes() {
        return this.nodes;
    }
    
    public Operator getWhere() {
        return this.where;
    }

    public void setLimit(long offset, long count) {
        this.offset = offset;
        this.count = count;
    }

    public void addAllFields() {
        this.freeze = true;
        if (this.nodes.size() == 1) {
            Node node = nodes.values().iterator().next();
            if (node.maker != null) {
                this.outputMeta = node.maker.getCursorMeta();
                return;
            }
            else if (node.planner != null) {
                this.outputMeta = node.planner.getOutputMeta();
                return;
            }
        }
        throw new IllegalArgumentException();
    }

    public int getWidth() {
        int result = 0;
        for (Node i:this.nodes.values()) {
            result += i.getWidth();
        }
        return result;
    }
}
