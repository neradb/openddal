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
// Created on 2014年4月24日
// $Id$

package com.openddal.route.rule;

import java.util.List;

import com.openddal.config.ShardedTableRule;
import com.openddal.route.algorithm.MultColumnPartitioner;
import com.openddal.route.algorithm.Partitioner;
import com.openddal.util.New;
import com.openddal.value.Value;

/**
 * @author jorgie.li
 */
public class RoutingCalculatorImpl implements RoutingCalculator {

    @Override
    public RoutingResult calculate(ShardedTableRule tableRouter, RoutingArgument arg) {
        ObjectNode[] partition = tableRouter.getObjectNodes();
        boolean typeof = tableRouter.getPartitioner() instanceof Partitioner;
        if (!typeof) {
            String name = tableRouter.getPartitioner().getClass().getName();
            throw new RuleEvaluateException("Algorithm " + name + " not type of " + Partitioner.class.getName());
        }
        Partitioner partitioner = (Partitioner)tableRouter.getPartitioner();
        switch (arg.getArgumentType()) {
            case RoutingArgument.NONE_ROUTING_ARGUMENT:
                return RoutingResult.fixedResult(partition);
            case RoutingArgument.FIXED_ROUTING_ARGUMENT:
                List<Value> values = arg.getValues();
                Value[] toArray = values.toArray(new Value[values.size()]);
                Integer[] position = partitioner.partition(toArray);
                checkReturnValue(tableRouter, position);
                List<ObjectNode> selected = New.arrayList();
                for (Integer integer : position) {
                    ObjectNode tableNode = partition[integer];
                    selected.add(tableNode);
                }
                return RoutingResult.fixedResult(selected);
            case RoutingArgument.RANGE_ROUTING_ARGUMENT:
                Value start = arg.getStart();
                Value end = arg.getEnd();
                position = partitioner.partition(start, end);
                checkReturnValue(tableRouter, position);
                List<ObjectNode> seleced = New.arrayList();
                for (Integer integer : position) {
                    ObjectNode tableNode = partition[integer];
                    seleced.add(tableNode);
                }
                return RoutingResult.fixedResult(seleced);
        }
        return null;
    }

    @Override
    public RoutingResult calculate(ShardedTableRule tableRouter, List<RoutingArgument> arguments) {
        ObjectNode[] partition = tableRouter.getObjectNodes();
        Object partitioner = tableRouter.getPartitioner();
        boolean typeof = partitioner instanceof MultColumnPartitioner;
        if (!typeof) {
            String name = partitioner.getClass().getName();
            throw new RuleEvaluateException("Algorithm " + name + " can't supported multiple rule column.");
        }
        MultColumnPartitioner cp = (MultColumnPartitioner) partitioner;
        Integer[] position = cp.partition(arguments);
        checkReturnValue(tableRouter, position);
        List<ObjectNode> selected = New.arrayList();
        for (Integer integer : position) {
            ObjectNode tableNode = partition[integer];
            selected.add(tableNode);
        }
        return RoutingResult.fixedResult(selected);

    }

    /**
     * @param tableRouter
     * @param positions
     * @throws RuleEvaluateException
     */
    private void checkReturnValue(ShardedTableRule tableRouter, Integer... positions) throws RuleEvaluateException {
        ObjectNode[] partition = tableRouter.getObjectNodes();
        String ptrName = tableRouter.getPartitioner().getClass().getName();
        if (positions == null) {
            String msg = String.format("The %s returned a illegal value null.", ptrName);
            throw new RuleEvaluateException(msg);
        }
        for (Integer position : positions) {
            if (position == null) {
                String msg = String.format("The %s returned a illegal value null.", ptrName);
                throw new RuleEvaluateException(msg);
            }
            if (position < 0 || position >= partition.length) {
                String msg = String.format("The %s returned a illegal value %d, it's out of table nodes bounds.",
                        ptrName, position);
                throw new RuleEvaluateException(msg);
            }
        }
    }

}
