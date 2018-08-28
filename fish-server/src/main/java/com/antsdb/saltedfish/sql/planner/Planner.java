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
import com.antsdb.saltedfish.sql.meta.PrimaryKeyMeta;
import com.antsdb.saltedfish.sql.meta.RuleMeta;
import com.antsdb.saltedfish.sql.meta.TableMeta;
import com.antsdb.saltedfish.sql.vdm.Aggregator;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.CursorMeta;
import com.antsdb.saltedfish.sql.vdm.CursorPrimaryKeySeek;
import com.antsdb.saltedfish.sql.vdm.DumbDistinctFilter;
import com.antsdb.saltedfish.sql.vdm.DumbGrouper;
import com.antsdb.saltedfish.sql.vdm.DumbSorter;
import com.antsdb.saltedfish.sql.vdm.FieldMeta;
import com.antsdb.saltedfish.sql.vdm.FieldValue;
import com.antsdb.saltedfish.sql.vdm.Filter;
import com.antsdb.saltedfish.sql.vdm.FullTextIndexMergeScan;
import com.antsdb.saltedfish.sql.vdm.FuncAvg;
import com.antsdb.saltedfish.sql.vdm.FuncCount;
import com.antsdb.saltedfish.sql.vdm.FuncGroupConcat;
import com.antsdb.saltedfish.sql.vdm.FuncMax;
import com.antsdb.saltedfish.sql.vdm.FuncMin;
import com.antsdb.saltedfish.sql.vdm.FuncSum;
import com.antsdb.saltedfish.sql.vdm.GroupByPostProcesser;
import com.antsdb.saltedfish.sql.vdm.IndexRangeScan;
import com.antsdb.saltedfish.sql.vdm.MasterRecordCursorMaker;
import com.antsdb.saltedfish.sql.vdm.NestedJoin;
import com.antsdb.saltedfish.sql.vdm.ObjectName;
import com.antsdb.saltedfish.sql.vdm.OpAnd;
import com.antsdb.saltedfish.sql.vdm.OpEqual;
import com.antsdb.saltedfish.sql.vdm.OpEqualNull;
import com.antsdb.saltedfish.sql.vdm.OpInSelect;
import com.antsdb.saltedfish.sql.vdm.OpInValues;
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
import com.antsdb.saltedfish.sql.vdm.RowKeyValue;
import com.antsdb.saltedfish.sql.vdm.TableRangeScan;
import com.antsdb.saltedfish.sql.vdm.TableScan;
import com.antsdb.saltedfish.sql.vdm.ThroughGrouper;
import com.antsdb.saltedfish.sql.vdm.Union;
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

    static {
        KEY.setColumnId(-1);
        KEY.setColumnName("*key");
        KEY.setType(DataType.binary());
        ROWID.setColumnId(0);
        ROWID.setColumnName("*rowid");
        ROWID.setType(DataType.longtype());
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
        for (ColumnMeta column : table.getColumns()) {
            PlannerField field = new PlannerField(node, column);
            field.setSourceTable(table.getObjectName());
            node.fields.add(field);
            this.rawMeta.addColumn(field);
        }
        this.last = this.current;
        this.current = node;
        return node.alias;
    }

    public ObjectName addCursor(String name, CursorMaker maker, boolean left, boolean isOuter) {
        Node node = new Node();
        node.maker = maker;
        node.alias = new ObjectName(null, name);
        if (left) {
            node.isOuter = isOuter;
        }
        else {
            // right join must be an outer join
            this.current.isOuter = true;
            node.isOuter = false;
        }
        this.nodes.put(node.alias, node);
        for (FieldMeta i : maker.getCursorMeta().getFields()) {
            PlannerField field = new PlannerField(node, i);
            node.fields.add(field);
            this.rawMeta.addColumn(field);
        }
        this.last = this.current;
        this.current = node;
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
        if (left) {
            Node node = this.nodes.get(alias);
            if (node == null) {
                throw new OrcaException("alias is not found: " + alias);
            }
            node.joinCondition = expr;
        }
        else {
            this.last.joinCondition = expr;
        }
    }

    public OutputField addOutputField(String name, Operator expr) {
        adjustOpMatch(expr);
        OutputField field = new OutputField(name, expr);
        this.fields.add(field);
        return field;
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

    public CursorMaker run() {
        if ((this.groupBy == null) && scanAggregationFunctions(getOutputFields())) {
            this.groupBy = Collections.emptyList();
        }
        
        analyze();
        Link path = build();

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

        // order by

        maker = buildOrderby(maker);

        // group by

        CursorMaker grouper = buildGroupBy(maker, path);
        boolean hasGrouper = grouper != maker;
        maker = grouper;
        
        // aggregation

        if (this.fields.size() > 0) {
            CursorMeta meta = new CursorMeta();
            List<Operator> exprs = new ArrayList<>();
            for (OutputField i : this.fields) {
                exprs.add(i.expr);
                FieldMeta field = new FieldMeta(i.name, i.expr.getReturnType());
                if (i.expr instanceof FieldValue) {
                    PlannerField pf = ((FieldValue) i.expr).getField();
                    field.setSourceTable(pf.getSourceTable());
                    field.setSourceColumnName(pf.getSourceName());
                    field.setTableAlias(pf.getTableAlias());
                }
                meta.addColumn(field);
            }
            maker = new Aggregator(maker, meta, exprs, this.ctx.getNextMakerId());
        }

        // group by post process

        if (hasGrouper) {
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

    private CursorMaker buildGroupBy(CursorMaker maker, Link path) {
        if (this.groupBy == null) {
            return maker;
        }
        if (this.groupBy.size() == 0) {
            // group the enter result set. happends when query has aggregation
            // function but no group by clause
            maker = new ThroughGrouper(maker);
            return maker;
        }
        List<PlannerField> groupKey = getGroupKey(this.groupBy);
        if (path.isUnique(groupKey)) {
            // result is unique by the key. no grouping is needed
            return maker;
        }
        maker = new DumbGrouper(maker, this.groupBy, ctx.getNextMakerId());
        return maker;
    }

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

    private CursorMaker buildOrderby(CursorMaker maker) {
        if (this.orderBy == null) {
            return maker;
        }
        if (hasImplicitGroupBy(maker)) {
            // skip order by if there is implicit group by clause
            return maker;
        }
        List<SortKey> sortKey = toSortKey(this.orderBy, this.orderByDirections);
        if (sortKey != null) {
            if (maker.setSortingOrder(sortKey)) {
                return maker;
            }
        }
        maker = new DumbSorter(maker, this.orderBy, this.orderByDirections, ctx.getNextMakerId());
        return maker;
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

    private boolean doesComplyOrder(TableScan maker, List<Operator> orderBy, List<Boolean> direction) {
        List<ColumnMeta> order = maker.getOrder();
        int idx = 0;
        for (int i = 0; i < order.size(); i++) {
            ColumnMeta column = order.get(i);
            Operator op = orderBy.get(idx);

            // match the key column with orderby column

            if (op instanceof FieldValue) {
                FieldMeta field = ((FieldValue) op).getField();
                if ((field.getColumn() == column) && direction.get(idx)) {
                    idx++;
                    if (idx == orderBy.size()) {
                        // all matched, perfect
                        return true;
                    }
                    continue;
                }
            }
            return false;
        }
        return false;
    }
    
    private boolean doesComplyOrder(RangeScannable maker, List<Operator> orderBy, List<Boolean> direction) {
        List<ColumnMeta> order = maker.getOrder();
        int idx = 0;
        for (int i = 0; i < order.size(); i++) {
            ColumnMeta column = order.get(i);
            Operator op = orderBy.get(idx);

            // match the key column with orderby column

            if (op instanceof FieldValue) {
                FieldMeta field = ((FieldValue) op).getField();
                if ((field.getColumn() == column) && direction.get(idx)) {
                    idx++;
                    if (idx == orderBy.size()) {
                        // all matched, perfect
                        return true;
                    }
                    continue;
                }
            }
            return false;
        }
        return false;
    }

    private boolean doesComplyOrder(CursorMaker maker, List<Operator> orderBy, List<Boolean> direction) {
        if (maker instanceof ThroughGrouper) {
            return doesComplyOrder(((ThroughGrouper) maker).getUpstream(), orderBy, direction);
        }
        else if (maker instanceof Aggregator) {
            return doesComplyOrder(((Aggregator) maker).getUpstream(), orderBy, direction);
        }
        else if (maker instanceof TableScan) {
            return doesComplyOrder((TableScan) maker, orderBy, direction);
        }
        else if (maker instanceof TableRangeScan) {
            return doesComplyOrder((RangeScannable) maker, orderBy, direction);
        }
        else if (maker instanceof IndexRangeScan) {
            return doesComplyOrder((RangeScannable) maker, orderBy, direction);
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
        for (PlannerField i : path.to.fields) {
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
        for (Node node : this.nodes.values()) {
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

        // if not eof go deeper

        link.previous = previous;
        Link result = build(link);
        return (result != null) ? result : link;
    }

    private void buildNodeJoinConditions(Link previous, Link link, List<ColumnFilter> filters) {
        if (previous != null) {
            for (ColumnFilter i : filters) {
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

    private Operator buildNodeFilters(Link link, List<ColumnFilter> filters) {
        List<ColumnFilter> result = new ArrayList<>();
        for (ColumnFilter i : filters) {
            if (i.isConstant && !link.consumed.contains(i)) {
                result.add(i);
            }
        }
        if (!result.isEmpty()) {
            return createFilterExpression(link, result);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
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
    }

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

    Link buildTableScan(Link previous, Node node) {
        Link result = build_(previous, node, Collections.emptyList());
        Operator filter = null;
        for (List<ColumnFilter> i:node.union) {
            filter = buildOr(filter, buildNodeFilters(result, i));
            buildNodeJoinConditions(previous, result, i);
        }
        if (filter != null) {
            result.maker = new Filter(result.maker, filter, ctx.getNextMakerId());
        }
        return result;
    }
    
    Link build(Link previous, Node node) {
        Link result = null;
        
        // no columns filters found, just build the full table scan
        
        if (node.union.size() <= 0) {
            return build_(previous, node, Collections.emptyList());
        }
        
        // union node, only proceed is it produced two non table scan

        List<Link> union = new ArrayList<>();
        
        for (List<ColumnFilter> i:node.union) {
            Link ii = build_(previous, node, i);
            
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

        // find the better plan between full table scan and union
        
        Link tablescan = buildTableScan(previous, node);
        if (tablescan.getScore() <= getScore(union)) {
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
        if (union.size() == 1) {
            result = union.get(0);
        }
        else {
            for (Link i:union) {
                if (result == null) {
                    result = i;
                }
                else {
                    Link merge = new Link(i.to);
                    merge.isUnion = true;
                    merge.maker = new Union(result.maker, i.maker, false, ctx.getNextMakerId());
                    result = merge;
                }
            }
        }
        return result;
    }
    
    Link build_(Link previous, Node node, List<ColumnFilter> filters) {
        Link link = null;

        // node is a record from outer query

        if (node.isParent) {
            link = new Link(node);
            link.maker = new MasterRecordCursorMaker(this.rawMeta.parent, ctx.getNextMakerId());
        }

        // try all keys/indexes

        if (link == null) {
            link = tryKeys(previous, node, filters);
        }

        // fall back to full table scan

        if (link == null) {
            link = new Link(node);
            if (node.table != null) {
                TableScan ts = new TableScan(node.table, ctx.getNextMakerId());
                ts.setNoCache(this.noCache);
                link.maker = ts;
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

    private Link tryKeys(Link previous, Node node, List<ColumnFilter> filters) {
        if (node.table == null) {
            return null;
        }

        // no filters, can't do table range scan

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
        for (int i = 0; i < columns.size(); i++) {
            ColumnMeta column = columns.get(i);
            boolean found = false;
            for (ColumnFilter filter : filters) {
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

    void analyze() {
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

    public ObjectName addTableOrView(String alias, Object table, boolean left, boolean isOuter) {
        ObjectName name = null;
        if (table instanceof TableMeta) {
            name = addTable(alias, (TableMeta) table, left, isOuter);
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

    public PlannerField findField(Predicate<FieldMeta> predicate) {
        PlannerField result = null;
        for (Node i : this.nodes.values()) {
            if (i.isParent) {
                continue;
            }
            for (PlannerField j : i.fields) {
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
                else if (it instanceof FuncAvg) {
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
}
