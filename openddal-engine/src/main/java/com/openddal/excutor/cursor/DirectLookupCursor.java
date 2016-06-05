package com.openddal.excutor.cursor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.openddal.command.dml.Select;
import com.openddal.config.TableRule;
import com.openddal.dbobject.index.IndexCondition;
import com.openddal.dbobject.table.Column;
import com.openddal.dbobject.table.Table;
import com.openddal.dbobject.table.TableFilter;
import com.openddal.dbobject.table.TableMate;
import com.openddal.engine.Constants;
import com.openddal.engine.Session;
import com.openddal.excutor.ExecutionFramework;
import com.openddal.message.DbException;
import com.openddal.result.Row;
import com.openddal.result.SearchRow;
import com.openddal.route.rule.RoutingResult;
import com.openddal.util.New;
import com.openddal.util.StringUtils;

public class DirectLookupCursor extends ExecutionFramework implements Cursor {

    private final Select select;
    private Cursor cursor;

    public DirectLookupCursor(Session session, Select select) {
        super(session);
        this.select = select;
    }


    @Override
    public void doPrepare() {
        RoutingResult routingResult = doRoute(select);
        selectNodes = routingResult.getSelectNodes();
        if (session.getDatabase().getSettings().optimizeMerging) {
            selectNodes = routingResult.group();
        }
    }

    protected RoutingResult doRoute(Select prepare) {
        ArrayList<TableFilter> topFilters = prepare.getTopFilters();
        for (TableFilter tf : topFilters) {
            TableMate table = (TableMate) tf.getTable();
            TableRule tableRule = table.getTableRule();
            if (tableRule.getType() != TableRule.GLOBAL_NODE_TABLE) {
                continue;
            }

        }
        return null;
    }


    @Override
    public Row get() {
        if (cursor == null) {
            return null;
        }
        return cursor.get();
    }

    @Override
    public SearchRow getSearchRow() {
        return cursor.getSearchRow();
    }

    @Override
    public boolean next() {
        while (true) {
            if (cursor == null) {
                nextCursor();
                if (cursor == null) {
                    return false;
                }
            }
            if (cursor.next()) {
                return true;
            }
            cursor = null;
        }
    }

    private void nextCursor() {

    }

    @Override
    public boolean previous() {
        throw DbException.throwInternalError();
    }

    public static boolean isDirectLookupQuery(Select select) {
        DirectLookupEstimator estimator = new DirectLookupEstimator(select.getTopFilters());
        return estimator.isDirectLookup();
    }

    private static class DirectLookupEstimator {

        private final ArrayList<TableFilter> filters;
        private Set<TableFilter> joinTableChain;

        private DirectLookupEstimator(ArrayList<TableFilter> topFilters) {
            this.filters = New.arrayList(topFilters.size());
            for (TableFilter tf : topFilters) {
                if (!StringUtils.startsWith(tf.getTableAlias(), Constants.PREFIX_JOIN)) {
                    filters.add(tf);
                }
            }
            for (TableFilter tf : filters) {
                if (tf.getTable() instanceof TableMate) {
                    continue;
                }
            }
        }

        private boolean isDirectLookup() {
            for (TableFilter tf : filters) {
                if (!tf.isFromTableMate()) {
                    return false;
                }
            }
            if (filters.size() == 1) {
                return true;
            } else {
                List<TableFilter> shardingTableFilter = New.arrayList();
                for (TableFilter tf : filters) {
                    if (!isGropTableFilter(tf)) {
                        return false;
                    }
                    TableMate table = (TableMate) tf.getTable();
                    if (table.getRuleColumns() != null) {
                        shardingTableFilter.add(tf);
                    }
                }
                if (shardingTableFilter.size() < 2) {
                    return true;
                }
                this.joinTableChain = New.hashSet();
                evaluationJoinChain(shardingTableFilter.iterator().next());
                return joinTableChain.containsAll(shardingTableFilter);
            }

        }

        private boolean isGropTableFilter(TableFilter filter) {
            TableMate table1 = (TableMate) filter.getTable();
            for (TableFilter item : filters) {
                if (item == filter) {
                    continue;
                }
                TableMate table2 = (TableMate) item.getTable();
                TableRule rule1 = table1.getTableRule();
                TableRule rule2 = table2.getTableRule();
                if (!rule1.isNodeComparable(rule2)) {
                    return false;
                }
            }
            return true;
        }

        private void evaluationJoinChain(TableFilter filter) {
            TableMate table1 = (TableMate) filter.getTable();
            Column[] columns1 = table1.getRuleColumns();
            if (columns1 == null) {
                throw new IllegalArgumentException("not sharding TableFilter");
            }
            ArrayList<IndexCondition> conditions = filter.getIndexConditions();
            int length = table1.getColumns().length;
            IndexCondition[] masks = new IndexCondition[length];
            for (IndexCondition condition : conditions) {
                int id = condition.getColumn().getColumnId();
                if (id >= 0) {
                    masks[id] = condition;
                }
            }

            if (masks != null) {
                List<Column> joinCols = New.arrayList();
                for (int i = 0, len = columns1.length; i < len; i++) {
                    Column column = columns1[i];
                    int index = column.getColumnId();
                    IndexCondition mask = masks[index];
                    if ((mask.getMask(conditions) & IndexCondition.EQUALITY) == IndexCondition.EQUALITY) {
                        Column compareColumn = mask.getCompareColumn();
                        if (compareColumn != null) {
                            joinCols.add(compareColumn);
                        }
                        if (i == columns1.length - 1) {
                            Set<Table> tables = New.hashSet();
                            for (Column column2 : joinCols) {
                                tables.add(column2.getTable());
                            }
                            for (Table table : tables) {
                                if (!(table instanceof TableMate)) {
                                    continue;
                                }
                                TableMate table2 = (TableMate) table;
                                Column[] columns2 = table2.getRuleColumns();
                                if (columns2 == null) {
                                    continue;
                                }
                                boolean contains = true;
                                for (Column column2 : columns2) {
                                    if (!joinCols.contains(column2)) {
                                        contains = false;
                                        break;
                                    }
                                }
                                if (contains) {
                                    joinTableChain.add(filter);
                                }
                                for (TableFilter tf : filters) {
                                    if (tf.getTable() == table && !joinTableChain.contains(filter)) {
                                        evaluationJoinChain(tf);
                                    }
                                }

                            }
                        }
                    }
                }
            }
        }

    }



}
