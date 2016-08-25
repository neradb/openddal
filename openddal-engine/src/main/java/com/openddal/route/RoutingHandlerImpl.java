/*
 * Copyright 2014-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the “License”);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Created on 2015年2月3日
// $Id$

package com.openddal.route;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.openddal.config.GlobalTableRule;
import com.openddal.config.ShardedTableRule;
import com.openddal.config.TableRule;
import com.openddal.dbobject.table.Column;
import com.openddal.dbobject.table.TableMate;
import com.openddal.engine.Database;
import com.openddal.result.SearchRow;
import com.openddal.route.rule.ObjectNode;
import com.openddal.route.rule.RoutingArgument;
import com.openddal.route.rule.RoutingCalculator;
import com.openddal.route.rule.RoutingCalculatorImpl;
import com.openddal.route.rule.RoutingResult;
import com.openddal.util.New;
import com.openddal.value.Value;

/**
 * @author jorgie.li
 */
public class RoutingHandlerImpl implements RoutingHandler {

    private RoutingCalculator trc;
    private Database database;

    public RoutingHandlerImpl(Database database) {
        this.database = database;
        this.trc = new RoutingCalculatorImpl();
    }

    @Override
    public RoutingResult doRoute(TableMate table) {
        TableRule tr = table.getTableRule();
        switch (tr.getType()) {
        case TableRule.SHARDED_NODE_TABLE:
            return fixedRoutingResult(((ShardedTableRule) tr).getObjectNodes());
        case TableRule.GLOBAL_NODE_TABLE:
            return ((GlobalTableRule) tr).getBroadcastsRoutingResult();
        case TableRule.FIXED_NODE_TABLE:
            return fixedRoutingResult(tr.getMetadataNode());
        default:
            throw new TableRoutingException(table.getName() + " does not support routing");
        }
    }

    @Override
    public RoutingResult doRoute(TableMate table, SearchRow row) {
        TableRule tr = table.getTableRule();
        switch (tr.getType()) {
        case TableRule.SHARDED_NODE_TABLE:
            try {
                return getRoutingResult(table, row);
            } catch (TableRoutingException e) {
                throw e;
            } catch (Exception e) {
                throw new TableRoutingException(table.getName() + " routing error.", e);
            }
        default:
            throw new TableRoutingException(table.getName() + " does not support routing");
        }
    }

    private RoutingResult getRoutingResult(TableMate table, SearchRow row) {
        ShardedTableRule tr = (ShardedTableRule) table.getTableRule();
        Column[] ruleCols = table.getRuleColumns();
        List<RoutingArgument> args = New.arrayList(ruleCols.length);
        for (Column ruleCol : ruleCols) {
            Value v = row.getValue(ruleCol.getColumnId());
            v = ruleCol.convert(v);
            RoutingArgument arg = new RoutingArgument(ruleCol.getName(), v);
            args.add(arg);
        }
        RoutingResult rr;
        if (args.size() == 1) {
            RoutingArgument argument = args.get(0);
            rr = trc.calculate(tr, argument);
        } else {
            rr = trc.calculate(tr, args);
        }
        if (rr.isMultipleNode()) {
            throw new TableRoutingException(table.getName() + " routing error.");
        }
        return rr;
    }

    @Override
    public RoutingResult doRoute(TableMate table, SearchRow first, SearchRow last, Map<Column, Set<Value>> inColumns) {
        TableRule tr = table.getTableRule();
        if (tr instanceof ShardedTableRule)
            try {
                Column[] ruleCols = table.getRuleColumns();
                List<RoutingArgument> args = New.arrayList(ruleCols.length);
                for (Column ruleCol : ruleCols) {
                    String ruleColName = ruleCol.getName();
                    RoutingArgument arg;
                    int idx = ruleCol.getColumnId();
                    Value startV = first == null ? null : first.getValue(idx);
                    Value endV = last == null ? null : last.getValue(idx);
                    if(startV != null && endV != null) {
                        //handle EQUAL,EQUAL_NULL_SAFE,IS_NULL
                        if(database.compare(startV, endV) == 0) {
                            // an X=? condition will produce less rows than
                            // an X IN(..) condition
                            startV = ruleCol.convert(startV);
                            arg = new RoutingArgument(ruleColName, startV);
                        } else if(inColumns.get(ruleCol) != null) {
                            Set<Value> values = inColumns.get(ruleCol);
                            ArrayList<Value> arrayList = convertValues(ruleCol, values);
                            arg = new RoutingArgument(ruleColName, arrayList);
                        } else {
                            startV = ruleCol.convert(startV);
                            endV = ruleCol.convert(endV);
                            arg = new RoutingArgument(ruleColName, startV, endV);
                        }
                        
                    } else if(inColumns.get(ruleCol) != null) {
                        Set<Value> values = inColumns.get(ruleCol);
                        ArrayList<Value> arrayList = convertValues(ruleCol, values);
                        arg = new RoutingArgument(ruleColName, arrayList);
                    } else if(startV != null || endV != null){
                        startV = startV == null ? null : ruleCol.convert(startV);
                        endV = endV == null ? null : ruleCol.convert(endV);
                        arg = new RoutingArgument(ruleColName, startV, endV);
                    } else {
                        arg = new RoutingArgument(ruleColName);
                    }                    
                    args.add(arg);
                }
                RoutingResult rr;
                if (args.size() == 1) {
                    RoutingArgument argument = args.get(0);
                    rr = trc.calculate((ShardedTableRule) tr, argument);
                } else {
                    rr = trc.calculate((ShardedTableRule) tr, args);
                }
                return rr;
            } catch (TableRoutingException e) {
                throw e;
            } catch (Exception e) {
                throw new TableRoutingException(table.getName() + " routing error.", e);
            }
        else
            throw new TableRoutingException(table.getName() + " does not support routing");

    }

    private RoutingResult fixedRoutingResult(ObjectNode... tableNode) {
        RoutingResult result = RoutingResult.fixedResult(tableNode);
        return result;
    }
    
    private ArrayList<Value> convertValues(Column ruleCol, Set<Value> values) {
        ArrayList<Value> arrayList = New.arrayList(values.size());
        for (Value value : values) {
            arrayList.add(ruleCol.convert(value));
        }
        return arrayList;
    }

}
