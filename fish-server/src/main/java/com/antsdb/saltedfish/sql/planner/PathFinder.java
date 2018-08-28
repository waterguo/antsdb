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
import java.util.Collection;
import java.util.List;

import com.antsdb.saltedfish.nosql.Statistician;
import com.antsdb.saltedfish.sql.meta.ColumnMeta;
import com.antsdb.saltedfish.sql.meta.IndexMeta;
import com.antsdb.saltedfish.sql.meta.PrimaryKeyMeta;
import com.antsdb.saltedfish.sql.meta.RuleMeta;
import com.antsdb.saltedfish.sql.vdm.CursorMaker;
import com.antsdb.saltedfish.sql.vdm.FieldValue;
import com.antsdb.saltedfish.sql.vdm.OpAnd;
import com.antsdb.saltedfish.sql.vdm.OpMatch;
import com.antsdb.saltedfish.sql.vdm.OpOr;
import com.antsdb.saltedfish.sql.vdm.Operator;

/**
 * 
 * @author *-xguo0<@
 */
class PathFinder {
    private Statistician stats;

    PathFinder(Statistician stats) {
        this.stats = stats;
    }
    
    Path path(Collection<Node> nodes, Operator where) {
        Analyzer2 analyzer = new Analyzer2(nodes);
        analyzer.analyzeWhere(clone(where));
        for (Node i:nodes) {
            if (i.joinCondition != null) {
                analyzer.analyzeJoin(clone(i.joinCondition), i);
            }
        }
        List<Join> joins = analyzer.getResult();
        if (joins.size() < 1) {
            throw new IllegalArgumentException();
        }
        else if (joins.size() == 1) {
            return path(joins.get(0));
        }
        else {
            PathUnion result = new PathUnion();
            for (Join i:joins) {
                Path ii = path(i);
                if (joins.size() > 1 && ii.getRoot() instanceof PathFullScan) {
                    // fall back to full scan if one of the join results full table scan
                    return buildFullScan(nodes, where);
                }
                result.add(ii);
            }
            return result;
        }
    }
    
    private Path buildFullScan(Collection<Node> nodes, Operator where) {
        Path previous = null;
        for (Node i:nodes) {
            QueryNode ii = new QueryNode();
            ii.node = i;
            ii.condition = i.joinCondition;
            Path iii = new PathFullScan(ii);
            iii.previous = previous;
            previous = iii;
        }
        return previous;
    }

    Path path(Join join) {
        return build(join.nodes, null);
    }
    
    Path build(List<QueryNode> nodes, Path previous) {
        Path link = null;
        for (QueryNode node : nodes) {
            // if node already been in the path, next

            if (previous != null) {
                if (previous.exists(node)) {
                    continue;
                }
            }

            // outer join node cant start the query
            
            if (node.isOuter() && (previous == null)) {
                continue;
            }
            
            // outer join node can't replace existing one

            if (node.isOuter() && (link != null)) {
                continue;
            }

            // build the link
            Path result = build(previous, node);

            // keep the link with lowest score

            if (link == null) {
                link = result;
            }
            else if (result.getScore(this.stats) < link.getScore(this.stats)) {
                link = result;
            }

            // don't score the rest if this is the parent query node

            if (node.isParent()) {
                break;
            }
        }

        // end of nodes

        if (link == null) {
            return null;
        }

        // if not eof go deeper

        link.previous = previous;
        Path result = build(nodes, link);
        return (result != null) ? result : link;
    }

    Path build(Path previous, QueryNode node) {
        Path result = build_(previous, node, node.filters);
        return result;
    }

    Path build_(Path previous, QueryNode node, List<ColumnFilter> filters) {
        Path result = null;

        // node is a record from outer query

        if (node.isParent()) {
            result = new PathMaster(node);
        }

        // try all keys/indexes

        if (result == null) {
            result = tryKeys(previous, node, filters);
        }

        // fall back to full table scan

        if (result == null) {
            result = new PathFullScan(node);
        }
        return result;
    }

    private Path tryKeys(Path previous, QueryNode node, List<ColumnFilter> filters) {
        List<RuleMeta<?>> keys = node.getKeys();
        if (keys == null) {
            return null;
        }

        // no filters, can't do table range scan

        if (filters == null || filters.size() <= 0) {
            return null;
        }

        // compare all keys for the best match
        Path best = null;
        for (RuleMeta<?> index : keys) {
            Path link = tryKey(previous, node, filters, index);
            if (link == null) {
                continue;
            }
            if (best == null) {
                best = link;
                continue;
            }
            if (link.getScore(this.stats) < best.getScore(this.stats)) {
                best = link;
            }
        }
        return best;
    }

    private Path tryKey(Path previous, QueryNode node, List<ColumnFilter> filters, RuleMeta<?> key) {
        boolean isFullText = isFullText(key);
        
        // no filters, can't do table range scan

        if (filters.size() <= 0) {
            return null;
        }

        // match the key columns with the table filters one by one following the
        // sequence defined in the key.

        Path result = null;
        List<ColumnFilter> consumed = new ArrayList<>();
        List<ColumnMeta> columns = key.getColumns(node.node.table);
        if (columns.size() < 1) {
            return null;
        }
        Range range = new Range(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            ColumnMeta column = columns.get(i);
            boolean found = false;
            for (ColumnFilter filter : filters) {
                if (isFullText) {
                    if (filter.op == FilterOp.MATCH) {
                        result = createPathFullTextScanner(node, (IndexMeta) key, filter);
                        if (result != null) {
                            consumed.add(filter);
                            return result;
                        }
                    }
                }
                else {
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
                        consumed.add(filter);
                    }
                }
            }
            if (!found) {
                break;
            }
        }

        // create the cursor maker if it is still null
        
        if (consumed.isEmpty()) {
            return null;
        }
        result = range.createPath(node, key, consumed);
        return result;
    }

    private boolean isFullText(RuleMeta<?> key) {
        if (key instanceof IndexMeta) {
            return ((IndexMeta)key).isFullText();
        }
        return false;
    }

    @SuppressWarnings("unused")
    private boolean isUnique(RuleMeta<?> key) {
        if (key instanceof PrimaryKeyMeta) {
            return true;
        }
        if (key instanceof IndexMeta) {
            return ((IndexMeta)key).isUnique();
        }
        return false;
    }
    
    private Path createPathFullTextScanner(QueryNode node, IndexMeta index, ColumnFilter filter) {
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
        return new PathFullText(node);
    }

    /** is the expression calculable with given path */
    private boolean checkColumnReference(Path previous, QueryNode node, Object expr) {
        if (expr instanceof CursorMaker) {
            return true;
        }
        if (expr instanceof List<?>) {
            return true;
        }
        boolean[] valid = new boolean[1];
        valid[0] = true;
        ((Operator)expr).visit(it -> {
            if (!valid[0]) {
                return;
            }
            if (it instanceof FieldValue) {
                FieldValue cv = (FieldValue) it;
                if (node.node.findField(cv.getField()) != null) {
                    return;
                }
                if (previous != null) {
                    if (previous.node.node.findField(cv.getField()) != null) {
                        return;
                    }
                }
                valid[0] = false;
            }
        });
        return valid[0];
    }

    private Operator clone(Operator op) {
        if (op instanceof OpAnd) {
            Operator and = (OpAnd)op;
            Operator x = clone(and.getChildren().get(0));
            Operator y = clone(and.getChildren().get(1));
            return new OpAnd(x, y);
        }
        else if (op instanceof OpOr) {
            Operator or = (OpOr)op;
            Operator x = clone(or.getChildren().get(0));
            Operator y = clone(or.getChildren().get(1));
            return new OpOr(x, y);
        }
        else {
            return op;
        }
    }
}
