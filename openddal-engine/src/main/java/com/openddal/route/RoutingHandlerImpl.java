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

import java.util.List;

import com.openddal.config.ShardedTableRule;
import com.openddal.config.TableRule;
import com.openddal.dbobject.index.IndexCondition;
import com.openddal.dbobject.table.Column;
import com.openddal.dbobject.table.TableMate;
import com.openddal.engine.Database;
import com.openddal.engine.Session;
import com.openddal.result.SearchRow;
import com.openddal.route.rule.ObjectNode;
import com.openddal.route.rule.RoutingArgument;
import com.openddal.route.rule.RoutingCalculator;
import com.openddal.route.rule.RoutingCalculatorImpl;
import com.openddal.route.rule.RoutingResult;
import com.openddal.util.New;
import com.openddal.value.Value;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class RoutingHandlerImpl implements RoutingHandler {

    private RoutingCalculator trc;

    public RoutingHandlerImpl(Database database) {
        this.trc = new RoutingCalculatorImpl();
    }

    @Override
    public RoutingResult doRoute(TableMate table) {
        TableRule tr = table.getTableRule();
        switch (tr.getType()) {
        case TableRule.SHARDED_NODE_TABLE:
            return fixedRoutingResult(((ShardedTableRule) tr).getObjectNodes());
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
                throw new TableRoutingException(table.getName() + " routing error.");
            }
        case TableRule.FIXED_NODE_TABLE:
            return fixedRoutingResult(tr.getMetadataNode());
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
            List<Value> value = New.arrayList(1);
            value.add(v);
            RoutingArgument arg = new RoutingArgument(value);
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
    public RoutingResult doRoute(Session session, TableMate table, List<IndexCondition> idxConds) {
        TableRule tr = table.getTableRule();
        if (tr instanceof ShardedTableRule)
            try {
                RoutingAnalyzer analysor = new RoutingAnalyzer(table, idxConds);
                if (analysor.isAlwaysFalse()) {
                    return RoutingResult.emptyResult();
                }
                Column[] ruleCols = table.getRuleColumns();
                List<RoutingArgument> args = New.arrayList(ruleCols.length);
                for (Column ruleCol : ruleCols) {
                    RoutingArgument arg = analysor.doAnalyse(session, ruleCol);
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
                throw new TableRoutingException(table.getName() + " routing error.");
            }
        else if (tr instanceof ShardedTableRule)
            return fixedRoutingResult(((ShardedTableRule) tr).getObjectNodes());
        else
            return fixedRoutingResult(tr.getMetadataNode());

    }

    private RoutingResult fixedRoutingResult(ObjectNode... tableNode) {
        RoutingResult result = RoutingResult.fixedResult(tableNode);
        return result;
    }

}
